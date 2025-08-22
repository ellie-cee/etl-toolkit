from EscMT.base import *
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit,SearchableDict
from EscMT.models import *
from EscMT.graphQL import *
from django.db.models import Q

class ProjectCreatorOptions:
    def additionalProductMetafields(self,record:SearchableDict):
        return []
    def additionalVariantMetafields(self,record:SearchableDict):
        return []
    def additionalOrderMetafields(self,record:SearchableDict):
        return []
    def additionalCustomerMetafields(self,record:SearchableDict):
        return []
    
class ShopifyOperation:
    def __init__(self,profile="default",sourceClass="source",processor:ProjectCreatorOptions=ProjectCreatorOptions()):
        self.processor = processor
        self.sourceClass = sourceClass
        self.profile = profile
        if shopify.ShopifyResource.site is None:
            shopifyInit(useProfile=profile)
        self.gql = self.setGql()
        self.groupsProcessed = 0
    def processRecord(self,record):
        pass
    def setGql(self):
        self.gql = GraphQL()
        
    @staticmethod
    def lookupItemId(itemGid):
        try:
            record = RecordLookup.objects.get(externalId=itemGid)
            if record.shopifyId!="":
                return record.shopifyId
            return None
        except:
            traceback.print_exc()
            return None
    
class ShopifyImporter(ShopifyOperation):
    def rowCount(self):
        self.groupsProcessed = self.groupsProcessed + 1
        return self.groupsProcessed
    def showGroup(self):
        print(f"Processed group {self.rowCount()}",file=sys.stderr)
    def run(self):
        pass
    
    def setGql(self):
        return GraphQL()
    def recordType(self):
        return None
    
    def hasValue(self,value):
        if value!="" and value is not None:
            return True
        return False
            
    def createRecords(self,recordKey,record:GqlReturn,createRecord=True,recordType=None,url="",parentId="",alt=""):
        if recordType is None:
            recordType = self.recordType()
        mappingRecord,created = RecordLookup.objects.get_or_create(recordKey=recordKey)
        if created:
            mappingRecord.recordKey = recordKey
            mappingRecord.recordType=recordType
        if self.sourceClass=="source":
            mappingRecord.url = url
            mappingRecord.externalId = record.get("id")
            mappingRecord.url = url
            mappingRecord.parentId=parentId
            mappingRecord.alt = alt
        else:
            mappingRecord.shopifyId = record.get("id")
            
        mappingRecord.save()
        if self.sourceClass=="source" and createRecord:
            sourceRecord,created = Record.objects.get_or_create(
                externalId=record.get("id")
            )
            
            sourceRecord.data = record
            sourceRecord.sourceClass = self.sourceClass
            sourceRecord.recordType = recordType
            sourceRecord.save()
    
    def createUniqueRecord(self,recordKey,itemId,recordType,url="",parentId="",alt=""):
        mappingRecord,created = RecordLookup.objects.get_or_create(recordKey=recordKey)
        if created:
            mappingRecord.externalId = itemId
            mappingRecord.recordType=recordType
            mappingRecord.url = url
            mappingRecord.parentId = parentId
            mappingRecord.alt = alt
            mappingRecord.save()
            
    def searchQuery(self):
        
        try:
            sortField = "numericId"
            if self.sourceClass != "source":
                sortField = "numericShopifyId"
            
            latest = RecordLookup.objects.filter(recordType=self.recordType()).order_by(f"-{sortField}").first()
            if latest is None:
                return ""
            sortValue = getattr(latest,sortField)
            if sortValue is None:
                return ""
            query = f"id:>{sortValue}"
            
            return query
        except:
            traceback.print_exc()
            return ""
        
class ShopifyConsolidator:
    def __init__(self,processor:ProjectCreatorOptions=ProjectCreatorOptions()):
        
        self.processor = processor
    def run(self,record:GqlReturn=None,recordId:int=None) -> tuple[Record,SearchableDict]:
        
        
        if record is not None:
            recordId = record.get("id")
            
        savedRecord = Record.objects.get(externalId=recordId)
        raw:SearchableDict = savedRecord.getData()
        return savedRecord,raw
        
        
    
class ShopifyCreator(ShopifyOperation):
    def recordType(self):
        return "generic"
    def run(self):
        recordIterator = Record.objects.filter(recordType=self.recordType(),shopifyId="")
        if self.sortOrder() == "desc":
            recordIterator = recordIterator.order_by("-numericId")
        for record in recordIterator.all():
            
            self.processRecord(record)
            
    def processRecord(self,record:Record)->RecordLookup:

        recordLookup = RecordLookup.objects.get(externalId=record.externalId)
        return recordLookup
    def sortOrder(self):
        return "asc"
class ShopifyDeleter(ShopifyOperation):
    def recordType(self):
        return "generic"
    def run(self):
        for record in Record.objects.filter(recordType=self.recordType()).filter(~Q(shopifyId="")).all()[:20]:
            self.processRecord(record)
            
    def processRecord(self,record:Record)->RecordLookup:

        recordLookup = RecordLookup.objects.get(externalId=record.externalId)
        return recordLookup
    
