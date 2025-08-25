from EscMT.base import *
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit,SearchableDict
from EscMT.models import *
from EscMT.graphQL import *
from django.db.models import Q

class ProjectCreatorOptions:
    def productMetafields(self,record:SearchableDict,metafields):
        return metafields
    def variantMetafields(self,record:SearchableDict):
        return metafields
    def orderMetafields(self,record:SearchableDict,metafields):
        return metafields
    def customerMetafields(self,record:SearchableDict,metafields):
        return metafields
    def orderName(self,number):
        return f"#{number}"
    def orderFinalizeConsolidated(self,consolidated):
        return consolidated
    def productFinalizeConsolidated(self,consolidated):
        return consolidated
    def productFinalizeConsolidated(self,consolidated):
        return consolidated
    def productTags(self,tags):
        return tags
    def orderTags(self,tags):
        return tags
    def customerTags(self,tags):
        return tags
    def defaultFulfillmentLocation(self):
        return None
    def taxesAsLineItem(self):
        return False
class ShopifyQueryGenerator:
    def __init__(self,sourceClass="source",useRecordType="generic"):
        self.sourceClass = sourceClass
        self.useRecordType = useRecordType
        
    def recordType(self):
        return self.recordType
    def searchQuery(self):
        try:
            sortField = "numericId"
            if self.sourceClass != "source":
                sortField = "numericShopifyId"
            print(sortField,self.sourceClass)
            latest = RecordLookup.objects.filter(recordType=self.useRecordType).order_by(f"-{sortField}").first()
            if latest is None:
                return ""
            sortValue = getattr(latest,sortField)
            if sortValue is None:
                return ""
            query = f"id:>{sortValue}"
            print(query)
            return query
        except:
            traceback.print_exc()
            return ""
class ShopifyOperation:
    def __init__(
            self,
            profile="default",
            sourceClass="source",
            processor:ProjectCreatorOptions=ProjectCreatorOptions(),
            queryGenerator:ShopifyQueryGenerator=ShopifyQueryGenerator(),
            limit=None
        ):
        queryGenerator.sourceClass = sourceClass
        self.limit = limit
        self.processed = 0
        self.processor = processor
        self.queryGenerator = queryGenerator
        self.queryGenerator.useRecordType = self.recordType()
        
        self.sourceClass = sourceClass
        self.profile = profile
        if shopify.ShopifyResource.site is None:
            shopifyInit(useProfile=profile)
        self.gql = self.setGql()
        self.groupsProcessed = 0
    def recordType():
        return "generic"
    def processRecord(self,record):
        pass
    def setGql(self):
        self.gql = GraphQL()
        
    @staticmethod
    def lookupItemId(itemGid):
        try:
            record = RecordLookup.objects.get(externalId=itemGid)
            if record is None:
                return itemGid
            if record.shopifyId!="":
                return record.shopifyId
            return None
        except:
            return itemGid
    @staticmethod
    def lookupItemByKey(recordKey,recordType)->RecordLookup:
        record = None
        try:
            record = RecordLookup.objects.filter(recordKey=recordKey,recordType=recordType).first()
        except:
            return None
        return record
    @staticmethod
    def gided(id,type):
        if isinstance(id,int) or not id.startswith("gid"):
            return f"gid://shopify/{type}/{id}"
        return id
    def processedRecord(self):
        self.processed = self.processed + 1
    def atLimit(self):
        self.processedRecord()
        if self.limit is not None and self.processed>=self.limit:
            return True
        return False
    
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
    def recordsAfter(self,cutoffDate):
        return []
    
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
            
        return record,mappingRecord
    
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
        return self.queryGenerator.searchQuery()
        
class ShopifyConsolidator:
    def __init__(self,processor:ProjectCreatorOptions=ProjectCreatorOptions()):
        
        self.processor = processor
    def run(self,record:GqlReturn=None,savedRecord:Record=None,recordId:int=None) -> tuple[Record,SearchableDict]:
        
        if savedRecord is not None:
            return savedRecord,savedRecord.getData()
        
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
            if self.atLimit():
                return
            
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
    
