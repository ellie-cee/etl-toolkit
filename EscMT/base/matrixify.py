import csv
import json
from .client import *
from .iterator import RecordIterator

class MatrixifyExporter(RecordIterator):
    def __init__(self):
        self.writer = csv.DictWriter(
            open(
                f"matrixify/{self.type()}.csv","w",
                delimiter=',',
                quotechar='"',
                fieldnames=self.fieldnames()
            )
        )
        self.writer.writeheader()
        
    def type(self):
        return "None"
    def fields(self):
        return []
    def run(self):
        
        remaining = len(records)
        count = 1
        persec = 0
        
        for record in self.records():
            if len(self.only)>0 and record.recordId not in self.only:
                continue
            self.process(record)
    def process(self,record):
        pass
            
            
                
            