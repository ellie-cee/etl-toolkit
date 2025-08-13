from EscMT.base import *
from .base import ShopifyImporter
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *

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
        

    def processRecord(self,product):
        shopifyId = product.get("id")
        if shopifyId is None:
            return
        record,created = Record.objects.get_or_create(
            externalId=shopifyId
        )
        if created:
            record.recordType="product"
            record.data = product.data
            record.sourceClass = self.sourceClass
            
        print(record.get("id")) 