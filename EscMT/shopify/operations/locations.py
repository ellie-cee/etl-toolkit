from EscMT.base import *
from .base import ShopifyImporter
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *

class ShopifyLocationImporter(ShopifyImporter):
    def setGql(self):
        return GraphQL()
    def run(self):
        print(self.searchQuery())
        for locationGroup in self.gql.iterable(
            """
            query getLocations($after:String,$query:String) {
                locations(first: 5,after:$after,query:$query) {
                    nodes {
                        id
                        name
                        address {
                            formatted
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {
                "after":None,
                "query":""
            }
        ):

            for location in locationGroup:
                print("dewqdewq")
                self.processRecord(location)
            self.showGroup() 
              
                    
    def recordType(self):
        return "location"           
    def processRecord(self,location:GqlReturn):
        print(location.get("id"))
        self.createRecords(" ".join(location.search("address.formatted")),location)