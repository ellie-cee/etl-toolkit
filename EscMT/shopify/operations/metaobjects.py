from EscMT.base import *
from .base import *
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *

class ShopifyMetaobjectDefinitionImporter(ShopifyImporter):
    def setGql(self):
        return GraphQL()
    def recordType(self):
        return "metaobjectDefinition"
    def run(self):
        print(self.searchQuery())
        for definitionGroup in self.gql.iterable(
            """
            query metaobjectDefinitions($after:String) {
                metaobjectDefinition(first: 50,after:$after) {
                    nodes {
                        access {
                            admin
                            storefont
                        }
                        description
                        displayName
                        displayKey
                        fieldDefinitions {
                            description
                            key
                            name
                            required
                            type
                            validations {
                                name
                                type
                                value
                            }
                        }
                        id
                        name
                        standardTemplate
                        type
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

            for definition in definitionGroup:
                self.processRecord(definitionGroup)
            self.showGroup() 
              
                    
    def processRecord(self,definition:GqlReturn):
        self.createRecords(" ".join(definition.get("displayKey")),definition)

class ShopifyMetaobjectDefinitionConsolidator(ShopifyConsolidator):
    def run(self,definition,definitionId):
        
        if definition is not None:
            if isinstance(definition,GqlReturn):
                definitionId = definition.get("id")
            else:
                definitionId = definition.externalId
        definitionRecord,raw = super().run(record=order,recordId=definitionId)
        
        input = {
            "definition":{
                "access":raw.get("access"),
                "description":raw.get("description"),
                "displayNameKey":raw.get("displayKey"),
                "fieldDefinitions":raw.get("fieldDefinitions"),
                "name":raw.get("name"),
                "type":raw.get("type")
            }
        }
        definitionRecord.consolidated = input
        definitionRecord.save()
        return input
class ShopifyMetaobjectDefinitionCreator(ShopifyCreator):
    def recordType(self):
        return "metaobjectDefinition"
        
    def processRecord(self,definition:Record):
        
        recordLookup = super().processRecord(order)
        consolidated = ShopifyMetaOojectDefinitionConsolidator().run(definitionId=definition.externalId)
        
        createResult = GraphQL().run(
            """
            mutation CreateMetaobjectDefinition($definition: MetaobjectDefinitionCreateInput!) {
                metaobjectDefinitionCreate(definition: $definition) {
                    metaobjectDefinition {
                        id
                        name
                        type
                        fieldDefinitions {
                            name
                            key
                        }
                    }
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }
            """,
            consolidated
        )
        definitionId = createResult.search("data.metaobjectDefinitionCreate.metaobjectDefinition.id")
        if definitionId is None:
            createResult.dump()
            sys.exit()
        definition.shopifyId = definitionId
        recordLookup.shopifyId = definitionId
        definition.save()
        recordLookup.save()
            
class ShopifyMetaobjectImporter(ShopifyImporter):
    def setGql(self):
        return GraphQL()
    def recordType(self):
        return "metaobjectDefinition"
    def run(self):
        print(self.searchQuery())
        for definitionGroup in self.gql.iterable(
            """
            query getMetaobjects($after:String) {
                metaobjects(first: 50,after:$after) {
                    nodes {
                        id
                        displayName
                        fields {
                            definition {
                                id
                            }
                            jsonValue
                            key
                            
                            
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

            for definition in definitionGroup:
                self.processRecord(definitionGroup)
            self.showGroup() 
              
                    
    def processRecord(self,definition:GqlReturn):
        self.createRecords(" ".join(definition.get("displayKey")),definition)

class ShopifyMetaobjectConsolidator(ShopifyConsolidator):
    def run(self,definition,definitionId):
        
        if definition is not None:
            if isinstance(definition,GqlReturn):
                definitionId = definition.get("id")
            else:
                definitionId = definition.externalId
        definitionRecord,raw = super().run(record=order,recordId=definitionId)
        
        input = {
            "definition":{
                "access":raw.get("access"),
                "description":raw.get("description"),
                "displayNameKey":raw.get("displayKey"),
                "fieldDefinitions":raw.get("fieldDefinitions"),
                "name":raw.get("name"),
                "type":raw.get("type")
            }
        }
        definitionRecord.consolidated = input
        definitionRecord.save()
        return input
class ShopifyMetaobjectjectDefinitionCreator(ShopifyCreator):
    def recordType(self):
        return "metaobjectDefinition"
        
    def processRecord(self,definition:Record):
        
        recordLookup = super().processRecord(order)
        consolidated = ShopifyMetaOojectDefinitionConsolidator().run(definitionId=definition.externalId)
        
        createResult = GraphQL().run(
            """
            mutation CreateMetaobjectDefinition($definition: MetaobjectDefinitionCreateInput!) {
                metaobjectDefinitionCreate(definition: $definition) {
                    metaobjectDefinition {
                        id
                        name
                        type
                        fieldDefinitions {
                            name
                            key
                        }
                    }
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }
            """,
            consolidated
        )
        definitionId = createResult.search("data.metaobjectDefinitionCreate.metaobjectDefinition.id")
        if definitionId is None:
            createResult.dump()
            sys.exit()
        definition.shopifyId = definitionId
        recordLookup.shopifyId = definitionId
        definition.save()
        recordLookup.save()