#!/usr/bin/env python3

from EscMT import *
from EscMT.batch import BatchOperation,BatchRecordIterator
from EscMT.models import *
from EscMT.shopify import *
from EscMT.shopify.operations import *
from EscMT.shopify.project import *
from argparse import ArgumentParser
import json
from functools import reduce
from jmespath import search as jsearch
from datetime import datetime
import subprocess
import psutil
import time
import signal
import atexit

parser = argparse.ArgumentParser()
parser.add_argument("--recordType",required=True)
parser.add_argument("--profile",default="default")
parser.add_argument("--segments",type=int,default=100)
parser.add_argument("--sourceClass",default="source")
parser.add_argument("--segment",type=int)
parser.add_argument("--tranch",default="1970")
parser.add_argument("--mode",default="start")
args = parser.parse_args()
vargs = vars(args)

processor = ProjectCreatorOptions()

class CreateBatch(BatchOperation):
    def __init__(self,**args):
        super().__init__(**args)
        self.setLogfile("creation.log")
        self.gql = GraphQL()
        
        #signal.signal(signal.SIGTERM,lambda signal,frame: self.db.close())
        def onExit():
            pass
        atexit.register(onExit)
    
    def startUpdates(self):
        migrationDB.cursor().execute("set autocommit=0;")
    def endUpdates(self):
        migrationDB.cursor().execute("commit;")
    def loadTranches(self):
        return ["INITIAL"]
    def getRecordCount(self):
        return Record.objects.filter(shopifyId="",tranch=self.arg("tranch"),recordType=self.arg("recordType")).count()
    def updateRecord(self,recordId,segment):
        migrationDB.cursor().execute(f"update Record set segment={segment} where externald='{recordId}';")
    def segmentBatch(self):
        records = 0
        super().segmentBatch(
            recordIterator=BatchRecordIterator(
                self.arg("recordType"),
                self.arg("tranch")
            )
        )
    def worker(self):
        super().worker(
            recordIterator=BatchRecordIterator(
                self.arg("recordType"),
                self.arg("tranch"),
                segment=self.arg("segment")
            )
        )
    def logDetail(self):
        return f"{self.arg('tranch','ALL')}:{self.arg('segment',0)}:{self.arg('mode')})"

    def processWorkerRecord(self, record:Record):
            
        print(f"processing record {record.externalId}")   
        match self.arg("recordType"):
            case "product":
                ShopifyProductCreator(
                    profile=self.arg("profile"),
                    processor=processor
                ).processRecord(record,reconsolidate=True)
            case "customer":
                ShopifyCustomerCreator(
                    profile=self.arg("profile"),
                    processor=processor
                ).processRecord(record,reconsolidate=True)
            case "order":
                deleted = GraphQL().run(
                    """
                    mutation deleteOrder($orderId:ID!) {
                        orderDelete(orderId: $orderId) {
                            deletedId
                            userErrors {
                                field
                                message
                                code
                            }
                        }
                    }
                    """,
                    {"orderId":record.externalId}
                ).getDataRoot()
                deletedOrderId = deleted.get("deletedId")
                try:
                    print(f"deleted order {deletedOrderId} https://admin.shopify.com/store/urbancarry-holsters/orders/{deletedOrderId.split('/')[-1]}",flush=True)
                except:
                    traceback.print_exc()
                shopifyOrder:GqlReturn = ShopifyOrderCreator(
                    processor=ProjectCreatorOptions()
                ).processRecord(record)
            
                newOrderId = shopifyOrder.search("data.orderCreate.order.id")
                if newOrderId is not None:
                    print(f"created order {newOrderId} https://admin.shopify.com/store/urbancarry-holsters/orders/{newOrderId.split('/')[-1]}",flush=True)
        
        
        
        
        
x = CreateBatch(**vargs)
x.run()





