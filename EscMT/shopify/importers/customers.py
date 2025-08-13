from EscMT.base import *
from .base import ShopifyImporter
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *

class ShopifyCustomerImporter(ShopifyImporter):
    def setGql(self):
        return Customer()
    
    def maxQuery(self):
        try:
            latest = Record.objects.order_by("-numericExternalId").first()
            if latest.numericExternalId is None:
                return ""
            query = f"id:>{latest.numericExternalId}"
            
            return query
        except:
            return ""
    def run(self):
        params = {
            "after":None,
            "query":self.maxQuery()
        }
        for customerGroup in self.gql.iterable(
            """
            query getCustomers($after:String,$query:String) {
                customers(first:250,after:$after,query:$query) {
                    nodes {
                        id
                        addresses {
                            
                            address1
                            address2
                            city
                            company
                            countryCodeV2
                            firstName
                            lastName
                            phone
                            provinceCode
                            timeZone
                            zip
                        }
                        defaultEmailAddress {
                            emailAddress
                        }                        
                        email
                        firstName
                        lastName
                        note
                        tags
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
           params
        ):

            for customer in customerGroup:
                self.processRecord(customer)
                
                    
                
    def processRecord(self,customer):
        email = customer.get("email")
        if email is None:
            email = customer.search("defaultEmailAddress.email")
            if email is None:
                return
            if self.sourceClass=="dest":
                emailMappingRecord,created = RecordLookup.objects.get_or_create(recordKey=email)
                if created:
                    emailMappingRecord.destShopifyId = customer.get("id")
                    emailMappingRecord.save()
                customerRecord,created = Record.objects.get_or_create(
                    externalId=customer.get("id")
                )
                if created:
                    customerRecord.data = customer
                    customerRecord.sourceClass = self.sourceClass
                    customerRecord.recordType = "customer"
                    customerRecord.save()
                    
                    
        