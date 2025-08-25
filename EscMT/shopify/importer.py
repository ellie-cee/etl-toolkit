from ..base import *
import mysql.connector
import os
from ..misc import jsonify,loadProfiles,shopifyInit
from ..models import *
from ..graphQL import *

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
    def processRecord(record):
        pass
    def setGql(self):
        return GraphQL()
    
class ShopifyCustomerImporter(ShopifyImporter):
    pass
class ShopifyOrderImporter(ShopifyImporter):
    pass
class ShopifyCustomerImporter(ShopifyImporter):
    def setGql(self):
        return Customer()
    
    def maxQuery(self):
        try:
            latest = RecordLookup.objects.latest("numericId")
            if latest.numericId is None:
                return ""
            query = f"id:>{latest.numericId}"
            
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
                
                email = customer.get("email")
                if email is None:
                    email = customer.search("defaultEmailAddress.email")
                if email is None:
                    continue
                if self.sourceClass=="dest":
                    emailMappingRecord,created = CustomerLookup.objects.get_or_create(email=email)
                    if created:
                        emailMappingRecord.customerId = customer.get("id")
                        emailMappingRecord.save()
            self.showGroup()

class ShopifyProductImporter(ShopifyImporter):
    def setGql(self):
        return Products()
    
    def run(self):
        for productGroup in self.gql.iterable(
            """
            query getProducts($after:String) {
                products(after:$after,first:250) {
                    nodes {
                        id
                        category {
                            #attributes(first:20) {
                            #    nodes {
                            #        ... on TaxonomyChoiceListAttribute {
                            #            
                            #            name
                            #            values(first:20) {
                            #                nodes {
                            #                    name
                            #                }
                            #            }
                            #        }
                            #        ... on TaxonomyMeasurementAttribute {
                            #            name
                            #            options {
                            #                key
                            #                value
                            #            }
                            #        }
                            #   }
                            #}
                            fullName
                            isArchived
                            isLeaf
                            isRoot
                            level
                            name
                        }
                        createdAt
                        description
                        descriptionHtml
                        featuredMedia {
                            alt
                            id
                            mediaContentType
                            preview {
                                image {
                                    altText
                                    id
                                    url
                                }
                            }
                            status
                        }
                        handle
                        giftCardTemplateSuffix
                        isGiftCard
                        media(first:25) {
                            nodes {
                                alt
                                id
                                mediaContentType
                                preview {
                                    image {
                                        altText
                                        id
                                        url
                                    }
                                }
                                status
                            }
                        }
                        options {
                            name
                            optionValues {
                                name
                                swatch {
                                    color
                                    image {
                                        image {
                                            url
                                        }
                                    }
                                }
                            }
                            
                        }
                        
                        productType
                        publishedAt
                        requiresSellingPlan
                        seo {
                            description
                            title
                        }
                        status
                        tags
                        templateSuffix
                        title
                        tracksInventory
                        variants(first:50) {
                            nodes {
                                availableForSale
                                barcode
                                displayName
                                id
                                image {
                                    altText
                                    url
                                }
                                inventoryQuantity
                                position
                                price
                                selectedOptions {
                                    name
                                    optionValue {
                                        name
                                        swatch {
                                            color
                                            image {
                                                image {
                                                    url
                                                }
                                            }
                                        }
                                    }
                                    value
                                }
                                sku
                                taxable
                                title
                                unitPrice {
                                    amount
                                    currencyCode
                                }
                                unitPriceMeasurement {
                                    measuredType
                                    quantityUnit
                                    quantityValue
                                    referenceUnit
                                    referenceValue
                                }
                            }
                        }
                        vendor 
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {"after":None}
        ):
            for product in productGroup:
                metafields = self.loadMetafields(product)
                self.processRecord(product)
            self.showGroup()
                
    def loadMetafields(self,product):
        record = self.gql.run(
            """
            query getProductMetafields($productId:ID!) {
                product(id:$productId) {
                    metafields(first:100) {
                        nodes {
                            namespace
                            key
                            value
                            jsonValue
                            reference {
                                ... on Metaobject {
                                    displayName
                                    fields {
                                        key
                                        type
                                        value
                                    }
                                    handle
                                }
                                ... on Collection {
                                    id
                                    handle
                                    description
                                    descriptionHtml
                                    title
                                }
                                ... on GenericFile {
                                    id
                                    alt
                                    fileStatus
                                    url
                                    mimeType                            
                                }
                                ... on MediaImage {
                                    id
                                    alt
                                    fileStatus
                                    image {
                                        alt:altText
                                        url
                                    }
                                }
                                ... on Product {
                                    id
                                    handle
                                }
                                ... on ProductVariant {
                                    id
                                    barcode
                                    sku
                                    displayName
                                    selectedOptions {
                                        name 
                                        optionValue {
                                            name
                                        }
                                    }
                                }
                                ... on Video {
                                    id
                                    alt
                                    fileStatus
                                    sources {
                                        url
                                    }
                                }
                            }
                        }
                    }
                    variants(first:100) {
                        nodes {
                            metafields(first:20) {
                                nodes {
                                    namespace
                                    key
                                    value
                                    jsonValue
                                    reference {
                                        ... on Metaobject {
                                            displayName
                                            fields {
                                                key
                                                type
                                                value
                                            }
                                            handle
                                        }
                                        ... on Collection {
                                            id
                                            handle
                                            description
                                            descriptionHtml
                                            title
                                        }
                                        ... on GenericFile {
                                            id
                                            alt
                                            fileStatus
                                            url
                                            mimeType                            
                                        }
                                        ... on MediaImage {
                                            id
                                            alt
                                            fileStatus
                                            image {
                                                alt:altText
                                                url
                                            }
                                        }
                                        ... on Product {
                                            id
                                            handle
                                        }
                                        ... on ProductVariant {
                                            id
                                            barcode
                                            sku
                                            displayName
                                            selectedOptions {
                                                name 
                                                optionValue {
                                                    name
                                                }
                                            }
                                        }
                                        ... on Video {
                                            id
                                            alt
                                            fileStatus
                                            sources {
                                                url
                                            }
                                        }
                                    }
                                    
                                }
                            }
                        }
                    }
                }
            }
            """,
            {"productId":product.get("id")}
        )
        product.set("metafields.nodes",[x.data for x in record.nodes("data.product.metafields")])
        recordVariants = record.nodes("data.product.variants")
        
        for index,variant in enumerate(product.nodes("variants")):
            variant.set("metafields.nodes",[x.data for x in recordVariants[index].nodes("metafields")])
        if len(record.nodes("data.product.metafields.nodes"))>0:
            record.dump()   
        

   