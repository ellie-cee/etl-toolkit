import pathlib
import sys
from EscMT.models import Record,CreatorInstance

from EscMT.misc import shopifyInit
from  EscMT.graphQL import GraphQL,Order
from argparse import ArgumentParser
import json
from functools import reduce
from jmespath import search as jsearch
from datetime import datetime
import time
from logging import Logger
import os

class OrderCreator:
    def __init__(self,**kwargs):
        self.args = kwargs
        
        self.worker = self.arg("segment",0)
        logPath = pathlib.Path("logs")
        if not logPath.exists():
            logPath.mkdir(mode=666)
        
        self.logFile = open("logs/creator.log","a")
        self.started = datetime.now()
        shopifyInit()
        
        self.pid = os.getpid()
        self.gqlPoints = 0
        
        
        self.instance = CreatorInstance.objects.create(
            recordClass="order",
            tranch = self.arg("tranch","1970"),
            segment = self.arg("segment",-1),
            pid = self.pid
        )
        
    def arg(self,key,default=None):
        value = None
        if hasattr(self.args,key):
            value = getattr(self.args,key)
        else:
            value = self.args.get(key)
            
        if value is None:
            return default
        return value

    
    def log(self,message):
        now = datetime.now()
        print(f"[{now}][worker={self.worker}:{self.pid}:{self.gqlPoints}] {message}",file=self.logFile,flush=True)

    def finish(self,records):
        timeSpent = datetime.now() - self.started
        self.instance.delete()
        self.log(f"run finished. Processed {records} records in {timeSpent}")
        sys.exit()
    def consolidator():
        return None
    
    def createOrder(self,payload):
        orderCreated = Order().createOrder(payload)
        self.gqlPoints = orderCreated.throttleRemaining()
        if self.gqlPoints<=self.arg("minThrottle",0):
            self.log(f"GraphQL points <={self.arg('minThrottle',1000)}")
            time.sleep(5)
        return orderCreated
    
    def run(self):
        processed = 0

        orderDetails = None
        tranch = self.arg("tranch")
        segment = self.arg("segment")
        externalId = self.arg("id")
        allowedStatuses = self.arg("statuses",["PAID"])
        allowedFulFillmentStatuses = self.arg("alowedSulfillments",[])
        consolidator = self.consolidator()
        
        if externalId is not None:
            orderDetails = Record.objects.filter(externalId=externalId).all()
        elif tranch is not None:
            if segment is not None:
                orderDetails = Record.objects.filter(
                    tranch=tranch,
                    segment=segment,
                    shopifyId__regex=r"^$"
                ).all()
            else:
                orderDetails = Record.objects.filter(
                    tranch=tranch,
                    shopifyId__regex=r"^$"
                ).all()

        orderGql = Order()
        self.log("Starting run")
        orderDetail:Record
        for orderDetail in orderDetails:
            if orderDetail.shopifyId is not None and orderDetail.shopifyId.strip() != "":
                continue
            orderInput = orderDetail.getData()
            paymentStatus = consolidator.paymentStatus(orderInput)
            if paymentStatus not in allowedStatuses:
                print(f"bad payment status {paymentStatus}")
                continue
            
            
            payload = consolidator.process(orderInput)
            orderDetail.consolidated = json.dumps(payload)
            orderDetail.save()
            
            result = self.createOrder(payload)
            orderId = result.search("data.orderCreate.order.id")
            
            if self.arg("dump"):
                result.dump()
                
            
            orderDetail.updated = datetime.now()
            
            if orderId is not None:
                self.log(f"Created order {orderId.split('/')[-1]} ({result.search('data.orderCreate.order.name','#000000')}) from Magento order {orderDetail.externalId}")
                orderDetail.shopifyId=orderId    
            if result.hasErrors():
                errorText = ", ".join(result.errorMessages())
                self.log(f"Unable to create order for Magento order {orderDetail.externalId}: {errorText}")
                orderDetail.errors = errorText
                
            orderDetail.save()
            processed = processed + 1
            limit = self.arg("limit",1000)
            if limit>=0 and processed>=limit:
                self.finish(processed)
                
        self.finish(processed)        
    def halt(self):
        pass
