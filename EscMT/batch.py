
from .models import *
from argparse import ArgumentParser
import json
from functools import reduce
from jmespath import search as jsearch
from datetime import datetime
import subprocess
import psutil
import time
import os
import sys
from slugify import slugify
import signal
from .misc import shopifyInit
from django.db.models import Q

class BatchRecordIterator:
    def __init__(self,type,tranch,segment=None):
        self.type = type
        self.tranch = tranch
        self.segment = segment
        recordIterator = Record.objects.filter(recordType=type).filter(shopifyId="").filter(tranch=tranch)
        if segment is not None:
            recordIterator = recordIterator.filter(segment=segment)
        
        self.recordIterator = recordIterator.iterator()
        #signal.signal(signal.SIGTERM,lambda signal,frame: self.db.close())
    def getRecordCount(self):
        return Record.objects.filter(shopifyId="",tranch=self.tranch,recordType=self.type).count()
    def __iter__(self):
        return self    
    def __next__(self):
        ret = None
        
        try:
            ret = next(self.recordIterator)
        except:
            traceback.print_exc()
            print("edwqdew")
            raise StopIteration
        return ret
        
class BatchRecordDeleteIterator(BatchRecordIterator):
    def __init__(self,type,tranch,segment=None):
        self.type = type
        self.tranch = tranch
        self.segment = segment
        recordIterator = Record.objects.filter(recordType=type,tranch=tranch).filter(~Q(shopifyId=""))
        print("dewqdewqd",type,tranch,segment)
        if segment is not None:
            recordIterator = recordIterator.filter(segment=segment)
        
        self.recordIterator = recordIterator.iterator()
        #signal.signal(signal.SIGTERM,lambda signal,frame: self.db.close())
    def getRecordCount(self):
        return Record.objects.filter(~Q(shopifyId="")).filter(tranch=self.tranch,recordType=self.type).count()
    
        
    
class BatchOperation:
    def __init__(self,**args):
        
        self.pid = os.getpid()
        
        self.instance = None
        self.scriptName = sys.argv[0]
        self.args = args
        for key,value in args.items():
            setattr(self,key,value)
        
        self.logFile = None
        self.slug = slugify(self.scriptName)
        self.processRecord = None
        shopifyInit(useProfile=self.arg("profile"))
        signal.signal(signal.SIGINT,lambda signal,frame: self.signalCaught())
        
        
    def run(self):
        match self.arg("mode",None):
            case "test":
                print("hey now")
            case "halt":
                self.signalCaught()
            case "start":
                self.beginBatch()
            case "segment":
                self.segmentBatch()
            case "worker":
                self.worker()
    def setLogfile(self,logfile):
        try:
            self.logFile = open(logfile,"a")
        except:
            pass
    def arg(self,key,default=None):
        if hasattr(self,key):
            return getattr(self,key)
        elif self.args.get(key) is not None:
            return self.args.get(key)
        return default
    def setArg(self,key,value):
        setattr(self.args,key,value)
        
    def createInstance(self,tranch="1970",segment=1):
        self.processRecord = CreatorInstance.objects.create(
            recordClass=self.slug,
            tranch=tranch,
            segment=segment,
            pid=self.pid
        )
    def logDetail(self):
        return ''
    def log(self,message,timestamp=None):
        if self.logFile is None:
            return
        if timestamp is None:
            timestamp = datetime.now()
        
        print(
            f"[{timestamp}][{self.pid}:{self.logDetail()}] {message}",
            file=self.logFile,
            flush=True
        )
    def getProcessCount(self):
        processCount = 0
        for instance in CreatorInstance.objects.filter(recordClass=self.slug).all():
            try:
                process = psutil.Process(instance.pid)
                name = process.name()
                processCount = processCount + 1
            except:
                continue
        return processCount
    def killWorkers(self):
        for instance in CreatorInstance.objects.filter(recordClass=self.slug).all():
            print(f"stopping instance {instance.pid}")
            os.system(f"kill -4 {instance.pid}")
            instance.delete()
    def killSpawner(self):
        for process in psutil.process_iter():
            try:
                commandLine = " ".join(process.cmdline())
                if self.scriptName in commandLine and "--mode start" in commandLine:
                    os.system(f"kill -9 {process.pid}")
            except:
                pass
    def signalCaught(self):
        
        match self.arg("mode","none"):
            case "start":
                self.killWorkers()
                sys.exit()
            case "segment":
                self.killWorkers()
                self.killSpawner()
                sys.exit()
            case "worker":
                if self.processRecord is not None:
                    self.processRecord.delete()
                    sys.exit()
                
                
    def loadTranches(self):
        return []
    def beginBatch(self):
        # spawn mother processes
        for tranch in sorted(self.loadTranches()):
            
            if self.arg("segments") is None:
                self.setArg("segments",50)
            
            operations = [
                self.scriptName,
                "--mode","segment",
                "--recordType",self.arg("recordType"),
                "--profile",self.arg("profile"),
                "--sourceClass",self.arg("sourceClass"),
                "--segments",str(self.arg("segments"))
            ]
            print(" ".join(operations))
            
            process = subprocess.Popen(
                operations
            )
            
            # wait for at least one process to spawn
            while self.getProcessCount()<1:
                time.sleep(1)
                print("hey nowp")
            # now we wait for them all to 
            
        while self.getProcessCount()>0:
            time.sleep(10)
    def getRecordCount(self):
        return 0
    def startUpdates():
        pass
    def endUpdates():
        pass
    def updateRecord(self,id,segment):
        pass
    def processWorkerRecord(self,record):
        pass
    def segmentBatch(self,recordIterator:BatchRecordIterator=None):
        
        if recordIterator is None:
            return
        
        recordCount = recordIterator.getRecordCount()
        segmentLength = int(recordCount/self.arg("segments",100))
        
        if segmentLength==0 and recordCount>0:
            segmentLength = recordCount
        elif recordCount<0:
            return
        processedRecords = 0
        currentSegment = 1
        segments = [1]
        self.startUpdates()
        record:Record
        totalRecordsCounted = 0
        for record in recordIterator:
            
            record.segment=currentSegment
            record.save()
            processedRecords = processedRecords +1
            if processedRecords % segmentLength == 0:
                if currentSegment+1<=self.arg("segments"):
                   
                    currentSegment = currentSegment + 1
                    segments.append(currentSegment)
        
        self.endUpdates()
        for segment in segments:
            os.system(f"nohup {self.scriptName} --mode worker --tranch {self.arg('tranch')} --segment {segment} --recordType {self.arg('recordType')} --profile {self.arg('profile')}>> logs/error.log 2>&1 &")
        
    def worker(self,recordIterator:BatchRecordIterator=None):
        print(f"Starting run worker {self.arg('segment')} pid {self.pid}")
        if recordIterator is None:
            return
        self.createInstance()
        start = time.time()
        processed = 0
        self.log("starting run")
        record:Record
        for record in recordIterator:
            if record.shopifyId!="":
                print(f"skipping {record.externalId}")
                continue
            self.processWorkerRecord(record)
            processed = processed + 1
        self.log(f"finished run {processed} records in {int(time.time()-start)}")
        self.processRecord.delete()
        sys.exit()
            
