from EscMT.base import *
from .base import ShopifyImporter,ShopifyCreator,ShopifyConsolidator,ShopifyDeleter
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *

class ShopifyCustomerImporter(ShopifyImporter):
    def setGql(self):
        return Customer()
    
    def run(self):
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
                            countryCode
                            firstName
                            lastName
                            phone
                            provinceCode
                            zip
                        }
                        defaultEmailAddress {
                            emailAddress
                        }
                        metafields(first:5) {
                            nodes {
                                namespace
                                key
                                value
                                type
                            }
                        }
                        email
                        firstName
                        lastName
                        note
                        tags
                        taxExempt
                        taxExemptions
                        
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
                "query":self.searchQuery()
            }
        ):

            for customer in customerGroup:
                self.processRecord(customer)
            self.showGroup() 
              
                    
    def recordType(self):
        return "customer"           
    def processRecord(self,customer):
        email = customer.get("email")
        if customer.get("id") is None:
            customer.dump()
        if email is None:
            email = customer.search("defaultEmailAddress.email")
            if email is None:
                return
        self.createRecords(email,customer)
        ShopifyCustomerConsolidator(processor=self.processor).run(customerId=customer.get("id"))
        
                    
class ShopifyCustomerConsolidator(ShopifyConsolidator):
    def run(self,customer=None,customerId=None):
        
        customerRecord,raw = super().run(record=customer,recordId=customerId)
        
        input = {
            "input":{
                "email":raw.get("email"),
                "addresses":raw.get("addresses"),
                "emailMarketingConsent":{
                    "marketingState":"NOT_SUBSCRIBED"
                },
                "firstName":raw.get("firstName"),
                "lastName":raw.get("lastName"),
                "note":raw.get("note"),
                "metafields":self.processor.additionalOrderMetafields()+raw.search("metafields.nodes"),
                "note":raw.get("note"),
                "tags":raw.get("tags")+["PET Customer"],
                "taxExempt":raw.get("taxExempt"),
                "taxExemptions":raw.get("taxExemptions")
            }
        }
        if False:
            phoneNumber = None
            for address in raw.get("addresses"):
                if address.get("phone") is not None:
                    phoneNumber = address.get("phone")
                    break
            if phoneNumber is not None:
                input["input"]["smsMarketingConsent"]={
                    "marketingState":"NOT_SUBSCRIBED",
                    "phone":phoneNumber
                }
        customerRecord.consolidated = input
        customerRecord.save()
        return input
        
class ShopifyCustomerCreator(ShopifyCreator):
    def recordType(self):
        return "customer"
    def processRecord(self,customer:Record,reconsolidate=True):
        print("processing record")
        recordLookup = super().processRecord(customer)
        consolidated = ShopifyCustomerConsolidator(processor=self.processor).run(customerId=customer.externalId)
        
        shopify = GraphQL().run(
            """
            mutation createCustomer($input:CustomerInput!) {
                customerCreate(input:$input) {
                    userErrors {
                        field
                        message
                    }
                    customer {
                        id
                    }
                }
            }
            """,
            customer.consolidated,
            throttle=5000
        )
        
        customerId = shopify.search("data.customerCreate.customer.id")
        if customerId is not None:
            recordLookup.shopifyId = customerId
            recordLookup.save()
            customer.shopifyId = customerId
            customer.save()
        else:
            shopify.dump()
class ShopifyCustomerDeleter(ShopifyDeleter):
    def run(self,record,all=False):
        if isinstance(record,SearchableDict):
            self.delete(record.get("id"))
        if all:
            for record in Record.objects.filter(recordType="order").all():
                if record.shopifyId!="":
                    #recordLookup = RecordLookup.objects.get(shopifyId=record.shopifyId)
                    
                    self.delete(record.shopifyId)
                    #record.shopifyId = ""
                    #record.save()
                    #recordLookup.save()
        else:
            self.delete(record.shopifyId)
            
            
    def delete(self,shopifyId):
        print(f"deleting {shopifyId}")
        print(shopifyId)
        ret =  GraphQL().run(
            """
            mutation customerDelete($id: ID!) {
                customerDelete(input: {id: $id}) {
                    shop {
                       id
                    }
                    userErrors {
                        field
                        message
                    }
                    deletedCustomerId
                }   
            }
            """,
            {"id":shopifyId}
        )
        