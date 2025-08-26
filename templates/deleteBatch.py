#!/usr/bin/env python3

from EscMT import *
from EscMT.batch import BatchOperation,BatchRecordIterator,BatchRecordDeleteIterator
from EscMT.models import *
from EscMT.shopify import *
from EscMT.shopify.operations import *
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
parser.add_argument("--mode")
args = parser.parse_args()
vargs = vars(args)

class DeleteBatch(BatchOperation):
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
            recordIterator=BatchRecordDeleteIterator(
                self.arg("recordType"),
                self.arg("tranch")
            )
        )
    def worker(self):
        super().worker(
            recordIterator=BatchRecordDeleteIterator(
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
                ShopifyProductDeleter(profile=self.arg("profile")).delete(record,reconsolidate=True)
            case "customer":
                ShopifyCustomerDeleter(profile=self.arg("profile")).delete(record.shopifyId)
        
        
        
        
        
x = DeleteBatch(**vargs)
x.run()





