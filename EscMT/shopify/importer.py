from ..base import *
import mysql.connector
import os
from ..misc import jsonify,loadProfiles,shopifyInit
from ..models import *
from ..graphQL import *

class ShopifyImporter:
    def __init__(self,profile="default",recordClassification="source"):
        self.recordClassification = recordClassification
        shopifyInit(profile)
        self.gql = Products()
    
class ShopifyCustomerImporter(ShopifyImporter):
    pass
class ShopifyOrderImporter(ShopifyImporter):
    pass
class ShopifyCustomerImporter(ShopifyImporter):
    pass

class ShopifyProductImporter(ShopifyImporter):
    def importRecords(self):
        
        for productGroup in self.gql.iterable(
            """
            query getProducts($after:String) {
                products(after:$after,limit:100) {
                    nodes {
                        id
                        tags
                        createdAt
                        description
                        descriptionHtml
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
                                }
                                
                            }
                        }
                        productType
                        publishedAt
                        requiresSellingPlan
                        status
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
                                metafields(first:50) {
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
                                        }
                                    }
                                }
                                position
                                price
                                selectedOptions {
                                    name
                                    optionValue
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
                        
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """
        ):
            for product in productGroup:
                pass