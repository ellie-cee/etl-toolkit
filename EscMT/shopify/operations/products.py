from EscMT.base import *
from .base import *
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
            query getProducts($after:String,$query:String) {
                products(after:$after,query:$query,first:250) {
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
                            position
                            optionValues {
                                name
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
                        
                        vendor 
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
            for product in productGroup:
                self.loadOverageItems(product)
                self.processRecord(product)
                
            self.showGroup()
            
                
    def loadOverageItems(self,product):
        record = self.gql.run(
            """
            query getProductMetafields($productId:ID!) {
                product(id:$productId) {
                    variants(first:50) {
                        nodes {
                            availableForSale
                            barcode
                            displayName
                            id
                            image {
                                id
                                altText
                                url
                            }
                            inventoryItem {
                                inventoryLevels(first:10) {
                                    nodes {
                                        location {
                                            id
                                        }
                                        quantities(names:["on_hand"]) {
                                            quantity
                                        }
                                    }
                                }    
                            }
                            
                            inventoryPolicy
                            inventoryQuantity
                            metafields(first:20) {
                                nodes {
                                    namespace
                                    key
                                    type
                                    value    
                                }
                            }
                            position
                            price
                            selectedOptions {
                                name
                                optionValue {
                                    name
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
                    metafields(first:10) {
                        nodes {
                            namespace
                            key
                            value
                            type
                        }
                    }
                    #variantMetafields:variants(first:100) {
                    #    nodes {
                    #        id
                    #    }
                    #}
                }
            }
            """,
            {"productId":product.get("id")}
        )
        
        product.set("metafields.nodes",[x.data for x in record.nodes("data.product.metafields")])
        #product.set("variantsMetafields.nodes",[x.data for x in record.nodes("data.product.variants")])
        product.set("variants.nodes",[x.data for x in record.nodes("data.product.variants")])
        recordVariants = record.nodes("data.product.variants")
        
        for index,variant in enumerate(product.nodes("variants")):
            variant.set("metafields.nodes",[x.data for x in recordVariants[index].nodes("metafields")])
        
        
    def handleMedia(self,product):
        pass
    def recordType(self):
        return "product"
    
    def createMediaRecord(self,mediaRecord):
        pass
        
    def processRecord(self,product:GqlReturn):
        handle = product.get("handle")
        if product.get("id") is None:
            product.dump()
            
        if handle is None:
            return
        print(product.get("id"),file=sys.stderr)
        # only create source records
        
        
        
        self.createRecords(handle,product)
        for variant in product.nodes("variants"):
            
            if variant.get("image") is not None:
                pass
            
            recordKey = None
            for recordKeyCandidate in ["sku","barcode","displayName"]:
                recordKey = variant.search(recordKeyCandidate)
                if recordKey is not None:
                    break
            self.createRecords(recordKey,variant,recordType="variant",parentId=product.get("id"),createRecord=False)
            
            
            
        #scan media items for source record
        if self.sourceClass == "source":
            ShopifyProductConsolidator(processor=self.processor).run(product)
            
class ShopifyProductConsolidator(ShopifyConsolidator):
    def recordType(self):
        return "product"
    
    def filterDudImages(self,images):
        ret = []
        for image in images:
            if image.get("originalSource")!="":
                ret.append(image)
        return ret
    def run(self,product:Record=None,productId=None):    
        
        if product is not None:
            if isinstance(product,GqlReturn):
                productId = product.get("id")
            else:
                productId=product.externalId
            
        
        productRecord,raw = super().run(recordId=productId)
        if productRecord is None:
        
            return
        
        
        supplementalMedia = []
        featuredMedia = raw.getAsSearchable("featuredMedia")
        if featuredMedia is not None:
            supplementalMedia = [
                {
                    "alt":featuredMedia.get("alt"),
                    "originalSource":featuredMedia.search("preview.image.url"),
                    "mediaContentType":featuredMedia.get("mediaContentType")
                }
            ]
        input = {
            "productInput":{
                "media":self.processMedia(raw.getAsSearchable("media.nodes")),
                "product":{
                    #"category":raw.get("category"),
                    "descriptionHtml":raw.get("descriptionHtml"),
                    "giftCard":raw.get("isGiftCard"),
                    "giftCardTemplateSuffix":raw.get("giftCardTemplateSuffix"),
                    "handle":raw.get("handle"),
                    "productOptions":self.mapProductOptions(raw.get("options")),
                    "productType":raw.get("productType"),
                    "status":"DRAFT",
                    "tags":raw.get("tags")+["petshop-import"],
                    "metafields":self.processor.additionalProductMetafields(raw)+self.mapMetafields(raw.search("metafields.nodes")),
                    "seo":raw.get("seo"),
                    "templateSuffix":raw.get("templateSuffix"),
                    "title":raw.get("title"),
                    "vendor":raw.get("vendor")
                }
            },
            "variantInput":self.mapVariants(raw.getAsSearchable("variants.nodes"))
        }
        
        productRecord.consolidated = input
        productRecord.save()
        return input
    def processMedia(self,media):
        ret = []
        for mediaItem in media:
            ret.append(
                {
                    "alt":mediaItem.get("alt"),
                    "mediaContentType":mediaItem.get("mediaContentType"),
                    "originalSource":mediaItem.search("preview.image.url")
                }
            )
        return ret
    def mapVariants(self,variantList:List[SearchableDict]):
        ret = []
        
        for variant in variantList:
            variantInput = {
                "barcode":variant.get("barcode"),
                "compareAtPrice":variant.get("compareAtPrice"),
                "inventoryPolicy":variant.get("inventoryPolicy"),
                "metafields":self.processor.additionalVariantMetafields(variant)+self.mapMetafields(variant.search("metafields.nodes")),
                "optionValues":[
                    {
                        "optionName":option.get("name"),
                        "name":option.search("optionValue.name")
                    } 
                    for option in variant.getAsSearchable("selectedOptions")
                ],
                "inventoryItem": {
                    "sku":variant.get("sku"),
                    "tracked":True,
                },
                "inventoryQuantities":self.mapInventoryQuantities(variant),
                "price":variant.get("price"),
                "taxable":variant.get("taxable"),
            }
            if False:#variant.get("image") is not None or False:
                imageRecord = RecordLookup.objects.get(externalId=variant.get("image.id"))
                if imageRecord is not None:
                    variantInput["mediaId"] = imageRecord.shopifyId
           
            ret.append(variantInput)
        return ret
            
    def mapInventoryQuantities(self,variant:SearchableDict):
        
        
        inventorQuantities = []
        for levels in [SearchableDict(x) for x in variant.search("inventoryItem.inventoryLevels.nodes",[])]:
            
           
            inventorQuantities.append(
                {
                    "availableQuantity":levels.search("quantities[0].quantity",0),
                    "locationId":ShopifyOperation.lookupItemId(levels.search("location.id"))
                }
            )
        return inventorQuantities
    def mapMetafields(self,metafields):
        return [
            {
                "namespace":metafield.get("namespace"),
                "key":metafield.get("key"),
                "type":metafield.get("type"),
                "value":metafield.get("value"),
                
            } 
            for metafield in filter(lambda x:x.get("type") not in ["list.metaobject_reference","metaobject_reference"],metafields)
        ]
    def mapProductOptions(self,options):
        return [{
            "name":option.get("name"),
            "position":option.get("position"),
            "values":option.get("optionValues"),
        } for option in options]
        
    
    


class ShopifyProductCreator(ShopifyCreator):
    def recordType(self):
        return "product"
    def processRecord(self,product:Record,reconsolidate=True):
        recordLookup = super().processRecord(product)
        consolidated = product.consolidated
        if reconsolidate:
            consolidated = ShopifyProductConsolidator(processor=self.processor).run(productId=product.externalId)
            product.save()
        else:
            if isinstance(consolidated,str):
                consolidated = json.loads(consolidated)
                
        productInput = consolidated.get("productInput")
        shopifyProduct = GraphQL().run(
            """
            mutation createProduct($media:[CreateMediaInput!],$product:ProductCreateInput) {
                productCreate(media:$media,product:$product) {
                    product {
                        id
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            productInput,
            throttle=5000
        )
        productId = shopifyProduct.search("data.productCreate.product.id")
        if productId is None:
            shopifyProduct.dump()
        else:
            
            recordLookup.shopifyId = productId
            recordLookup.save()
            product.shopifyId = productId
            product.save()
            
            productVariants = GraphQL().run(
                """
                mutation profuctVariantsCreate($productId:ID!,$variants:[ProductVariantsBulkInput!]!) {
                    productVariantsBulkCreate(productId: $productId, variants: $variants,strategy:REMOVE_STANDALONE_VARIANT) {
                        productVariants {
                            id
                            title
                            sku
                            price
                            selectedOptions {
                                name
                                value
                            }
                        }
                        userErrors {
                            field
                            message
                        }
                    }
                }
                """,
                {
                    "productId":productId,
                    "variants":consolidated.get("variantInput")
                },
                throttle=5000
                
            )
            
            firstId = productVariants.search("data.productVariantsBulkCreate.productVariants[0].id")
            if firstId is None:
                productVariants.dump()
            else:
                print("processing Variants")
                for variant in productVariants.search("data.productVariantsBulkCreate.productVariants"):
                    try:
                      
                        existingRecord = RecordLookup.objects.filter(
                        recordKey=variant.get("sku")
                        ).first()
                        if existingRecord is not None:
                      
                            existingRecord.shopifyId = variant.get("id")
                            existingRecord.save()
                    except:
                        productVariants.dump()
            print(f"Created record {productId}",flush=True)
                        
class ShopifyProductDeleter:
    def run(self,record:Record=None,all=False):
        if all:
            for record in Record.objects.all():
                if record.shopifyId!="":
                    self.delete(record.shopifyId)
                    record.shopifyId = ""
        else:
            self.delete(record.shopifyId)
            
            
    def delete(self,shopifyId):
        return GraphQL().run(
            """
            """,
            {"orderId":shopifyId}
        )
        
class ShopifyProductSync(ShopifyOperation):
    def run(self,sourceProfile="default",destProfile="dest"):
        for record in Record.objects.filter(recordType="product").filter(~Q(shopifyId="")).all():
            shopifyInit(useProfile=sourceProfile)
            variants = GraphQL().run(
                """
                query getProduct($productId:ID!) {
                    product(id:$productId) {
                        id
                        variants(first:100) {
                            nodes {
                                id
                                inventoryItem {
                                    inventoryLevels(first:10) {
                                        nodes {
                                            location {
                                                id
                                            }
                                            quantities(names:["on_hand"]) {
                                                quantity
                                            }
                                        }
                                    }    
                                }
                                price
                            }
                        }
                    }
                }
                """,
                {"productId":record.externalId}
            )
            try:
                self.processRecord(variants,destProfile=destProfile)
            except:
                continue
    def processRecord(self, record:GqlReturn,destProfile="dest"):
        
        
        updateInput = []
        
        
        
        for variant in record.nodes("data.product.variants"):
            inventoryUpdates = [
                {
                    "quantity":x.search("quantities[0].quantity"),
                    "locationId":ShopifyOperation.lookupItemId(x.search("location.id")),
                                    
                } for x in variant.nodes("inventoryItem.inventoryLevels")
            ]
            updateInput.append(
                {
                    "id":ShopifyOperation.lookupItemId(variant.get("id")),
                    "price":variant.get("price")
                }
            )
           
        
        shopifyInit(useProfile=destProfile)
        productVariants = GraphQL().run(
            """
            mutation productVariantsUpdate($productId:ID!,$variants:[ProductVariantsBulkInput!]!) {
                productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                    productVariants {
                        id
                        title
                        sku
                        price
                        selectedOptions {
                            name
                            value
                        }
                        inventoryItem {
                            id
                        }
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {
                "productId":ShopifyOperation.lookupItemId(record.search("data.product.id")),
                "variants":updateInput
            },
        )
        print(ShopifyOperation.lookupItemId(record.search("data.product.id")))
        
        variantInventoryUpdateItems = []
        
        for index,variant in enumerate(productVariants.getAsSearchable("data.productVariantsBulkUpdate.productVariants")):
            variantUpdateItem = inventoryUpdates[index]
            variantUpdateItem["inventoryItemId"] = variant.search("inventoryItem.id")
            variantInventoryUpdateItems.append(variantUpdateItem)
            
        inventoryUpdate = GraphQL().run(
            """
            mutation InventorySet($input: InventorySetQuantitiesInput!) {
                inventorySetQuantities(input: $input) {
                    inventoryAdjustmentGroup {
                        createdAt
                        reason
                        referenceDocumentUri
                        changes {
                           name
                         delta
                        }
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {   "input":{
                    "ignoreCompareQuantity":True,
                    "name":"available",
                    "reason":"correction",
                    "quantities":variantInventoryUpdateItems,
                }
            }
        )
        
        
            
        
        