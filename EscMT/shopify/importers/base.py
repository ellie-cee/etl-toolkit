from EscMT.base import *
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *

class ShopifyImporter:
    def __init__(self,profile="default",sourceClass="source"):
        self.sourceClass = sourceClass
        self.profile = profile
        shopifyInit(useProfile=profile)
        self.gql = self.setGql()
        self.groupsProcessed = 0
    def rowCount(self):
        self.groupsProcessed = self.groupsProcessed + 1
        return self.groupsProcessed
    def showGroup(self):
        print(f"Processed group {self.rowCount()}",file=sys.stderr)
    def run(self):
        pass
    def processRecord(self,record):
        pass
    def setGql(self):
        return GraphQL()