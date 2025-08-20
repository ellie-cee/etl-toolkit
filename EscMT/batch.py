
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

class BatchRecordIterator:
    def __init__(self,db,type,tranch,segment=None):
        super().__init__()
        recordIterator = Record.objects.filter(recordType=type,shopifyId="",tranch=tranch)
        if segment is not None:
            recordIterator = recordIterator.filter(segment=segment)
        self.recordIterator = recordIterator.iterator()
        signal.signal(signal.SIGTERM,lambda signal,frame: self.db.close())

    def __next__(self):
        ret = None
        try:
            ret = next(self.recordIterator.iterator())
        except:
            raise StopIteration
        return ret
        
    
        
    
class BatchOperation:
    def __init__(self,**args):
        
        self.pid = os.getpid()
        
        self.instance = None
        self.scriptName = sys.argv[0]
        self.args = args
        for key,value in vars(args).items():
            setattr(self,key,value)
        
        self.logFile = None
        self.slug = slugify(self.scriptName)
        self.processRecord = None
        shopifyInit()
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
            print(self.scriptName)
            operations = [
                self.scriptName,
                "--mode","segment"
            ]
            if self.args.segments is None:
                self.args.segments = 50
                
            for k,v in self.args.items():
                operations = operations + [f"--{k}",v]

            process = subprocess.Popen(
                operations
            )
            
            # wait for at least one process to spawn
            while self.getProcessCount()<1:
                time.sleep(1)
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
        segmentLength = int(self.getRecordCount()/self.arg("segments",100))
        
        processedRecords = 0
        currentSegment = 1
        segments = [1]
        self.startUpdates()
        for record in recordIterator:
            self.updateRecord(record.get("externalId"),currentSegment)
            processedRecords = processedRecords +1
            if processedRecords % segmentLength == 0:
                if currentSegment+1<=self.arg("segments"):
                    print(f"new segment {currentSegment}")
                    currentSegment = currentSegment + 1
                    segments.append(currentSegment)
        
        self.endUpdates()
        
        for segment in segments:
            os.system(f"nohup {self.scriptName} --mode worker --tranch {self.arg('tranch')} --segment {segment} >> /tmp/error.log 2>&1 &")
        
        
    def worker(self,recordIterator:BatchRecordIterator=None):
        if recordIterator is None:
            return
        self.createInstance()
        start = time.time()
        processed = 0
        self.log("starting run")
        for record in recordIterator:
            self.processWorkerRecord(record)
            processed = processed + 1
        self.log(f"finished run {processed} records in {int(time.time()-start)}")
        self.processRecord.delete()
            
