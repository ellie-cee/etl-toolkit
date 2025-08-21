import functools
from EscMT.base import *
from .base import ShopifyImporter,ShopifyCreator,ShopifyConsolidator,ShopifyDeleter,ShopifyOperation
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *
import signal
import time
from django.db.models import Q
import random

def close_db(signal,frame):
    sys.exit()
    
signal.signal(signal.SIGINT,close_db)   
signal.signal(signal.SIGTERM,close_db)

class ShopifyOrderImporter(ShopifyImporter):
    def setGql(self):
        return Order()
    def recordType(self):
        return "order"
    def gqlQuery(self):
        return """
        billingAddress {
            address1
            address2
            city
            countryCode
            firstName
            lastName
            provinceCode
            phone                       
            zip
        }
        shippingAddress {
            address1
            address2
            city
            countryCode
            firstName
            lastName
            provinceCode
            phone
            zip
        }
        currentTotalWeight
        customAttributes {
            key
            value
        }
        closed
        closedAt
        confirmationNumber
        confirmed
        currencyCode
        customer {
            id
            firstName
            lastName
            email
            phone
            addresses {
                address1
                address2
                city
                zip
                country
                firstName
                lastName
                province
            }
        }
        discountApplications(first:10) {
            nodes {
                targetType
                targetSelection
                allocationMethod
                index
                value {
                    ... on MoneyV2 {
                        amount
                        currencyCode
                    }
                    ... on PricingPercentageValue {
                        percentage
                    }
                }
            }
        }
        discountCode
        discountCodes
        email
        
        id
        lineItems(first:100) {
            nodes {
                customAttributes {
                    key
                    value
                }
                discountAllocations {
                    discountApplication{
                        targetSelection
                        value {
                            ... on MoneyV2 {
                                amount
                                currencyCode
                            }
                            ... on PricingPercentageValue {
                                percentage
                            }
                        }
                    }
                }
                id
                image {
                    altText
                    url
                }
                isGiftCard
                priceSet: originalTotalSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                }
                quantity
                requiresShipping
                sku
                taxLines {
                    priceSet {
                        shopMoney {
                            amount
                            currencyCode
                        }
                    }
                    rate
                    title
                }
                title
                variant {
                    inventoryItem {
                        variant {
                            id
                        }
                            
                    }
                }
                vendor
            }
        }
        name
        note
        number
        processedAt
        sourceName
        subtotalPriceSet {
            shopMoney {
                amount
                currencyCode
            }
        }
        tags
        taxesIncluded
        taxLines {
            priceSet {
                shopMoney {
                    amount
                    currencyCode
                }
            }
            rate
            title
            
        }
        test     
        unpaid
        """
    def stubs(self):
        for orderGroup in GraphQL().iterable(
            """
            query getOrders($after:String) {
                orders(after:$after,first:250) {
                    nodes {
                        id
                        name
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {
                "after":None
            }
        ):
            for order in orderGroup:
                recordLookup = RecordLookup.objects.create(
                    externalId=order.get("id"),
                    recordType="order",
                    recordKey=order.get("name"),
                )
                record = Record.objects.create(
                    externalId=order.get("id"),
                    recordType="order",
                    sourceClass="source",
                    data={},
                )
                record.save()
                recordLookup.save()
                
    def single(self,shopifyId):
        ret = GraphQL().run(
            """
            query getOrder($orderId:ID!) {
                order(id:$orderId) {
                    %s
                }
            }
            """ % (self.gqlQuery()),
            {"orderId":shopifyId}
        )
       
        self.processRecord(GqlReturn(ret.search("data.order")))
    def run(self):
        processedCount = 0
        
        for orderGroup in self.gql.iterable(
            """
            query getOrders($after:String,$query:String) {
                orders(after:$after,query:$query,first:200) {
                    nodes {                        
                    %s
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """ % (self.gqlQuery()),
            {
                "after":None,
                "query":self.searchQuery()
            }
        ):
            
            order:GqlReturn
            for order in orderGroup:
                self.processRecord(order)
            processedCount = processedCount + 1
            
            
                
    def processRecord(self, order):
        
        try:
            overageItems:dict = self.loadApiOverageItems(order.get("id")).search("data.order")
            for overageItemId,overageItemValue in overageItems.items():
                order.set(overageItemId,overageItemValue)
        except:
            print(f"no API overage items for {order.get("id")}")
            
            
        self.createRecords(order.get("name"),order)
        print(f"order {order.get('id')}")
        ShopifyOrderConsolidator(processor=self.processor).run(order=order)
        
                    
                    
    def loadApiOverageItems(self,orderId)->GqlReturn:
        ret = self.gql.run(
            """
            query getOrder($orderId:ID!) {
                order(id:$orderId) {
                    id
                    transactions(first:20) {                            
                        amountSet {
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        authorizationCode
                        
                        kind
                        processedAt
                        status
                        test

                    }
                    metafields(first:5) {
                        nodes {
                            namespace
                            key
                            value
                            type
                        }
                    }
                    shippingLines(first:20) {
                        nodes {
                            code
                            source
                            title
                            priceSet: discountedPriceSet {
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                            }
                            taxLines {
                                priceSet {
                                    shopMoney {
                                        amount
                                        currencyCode
                                    }
                                }
                                rate
                                title
                            }
                        }
                        
                    } 
                    fulfillments(first:20) {
                        status
                        createdAt
                        displayStatus
                        id
                        fulfillmentLineItems(first:50) {
                            nodes {
                                id
                                quantity
                                lineItem {
                                    id
                                    quantity
                                    sku
                                    variant {
                                        id
                                    }
                                }
                            }
                        }
                        location {
                            id
                        }
                        requiresShipping
                        totalQuantity
                        trackingInfo {
                            number
                        }
                    }                                 
                }
            }
            """,
            {
                "orderId":orderId
            }
        )
        if ret.search("data.order.id") is None:
            ret.dump()
            sys.exit()
        return ret
        
class ShopifyOrderConsolidator(ShopifyConsolidator):
    def run(self,order=None,orderId=None):
        if order is not None:
            if isinstance(order,GqlReturn):
                orderId = order.get("id")
            else:
                orderId = order.externalId
        orderRecord,raw = super().run(record=order,recordId=orderId)
        
        
        
        consolidatedOrder = {
                "billingAddress":raw.get("billingAddress"),
                "buyerAcceptsMarketing":False,
                "currency":raw.get("currencyCode"),
                "customAttributes":raw.get("custmoAttributes"),
                "email":raw.get("email"),
                "fulfillmentStatus":self.calculateFulfillment(raw),
                "customAttributes":raw.get("customAttributes"),
                "lineItems":self.processLineItems(
                      [SearchableDict(lineItem) for lineItem in raw.search("lineItems.nodes")]  
                ),
                "metafields":self.processor.additionalOrderMetafields(raw)+raw.search("metafields.nodes",[]),
                "name":f"PS-{raw.get('number')}",
                "processedAt":raw.get("processedAt"),
                "shippingAddress":raw.get("shippingAddress"),
                "shippingLines":raw.search("shippingLines.nodes"),
                "tags":["PET order"]+raw.get("tags"),
                "taxesIncluded":raw.get("taxesIncluded"),   
                "taxLines":raw.get("taxLines"),
                "transactions":raw.get("transactions"),    
            }
        shopifyCustomerId = ShopifyOperation.lookupItemId(raw.search("customer.id"))
        
        if shopifyCustomerId is not None:
                consolidatedOrder["customer"] = {
                    "toAssociate":{
                        "id":shopifyCustomerId
                    }
                }
        else:
                customerInfo = raw.getAsSearchable("customer")
                consolidatedOrder["customer"] = {
                    "toUpsert":{
                        "addresses":customerInfo.get("addresses"),
                        "firstName":customerInfo.get("firstName"),
                        "lastName":customerInfo.get("lastName"),
                        "email":customerInfo.get("email")
                    }
                }
        
        ## for the time being we'll just stick to line-items
           
        discountCodes = raw.get("discountCodes")
        if discountCodes is None:
            discountCodes = []
        discountCode = raw.get("discountCode")
        
        def generateDiscountCode(type="Discount"):
            return f"{type} {random.randint(10,400)}"
        discountInput = None

        discountApplications = []
        for index,application in enumerate([SearchableDict(x) for x in raw.search("discountApplications.nodes")]):
            if application.get("targetSelection")!="ALL":
                continue
            try:
                discountCode = discountCodes[index]
            except:
                discountCode = generateDiscountCode("Discount")
            if application.search("value.percentage") is not None:
                discountInput = {
                    "itemPercentageDiscountCode":{
                        "code":discountCode,
                        "percentage":application.search("value.percentage")
                    }
                }
            else:
                discountInput = {
                    "itemFixedDiscountCode":{
                        "code":discountCode,
                        "amountSet":application.search("value.percentage")
                    }
                }
                    
            
                
        if discountInput is not None:
            consolidatedOrder["discountCode"] = discountInput
            
        input = {
            "options":{
                "inventoryBehaviour":"BYPASS",
                "sendFulfillmentReceipt":False,
                "sendReceipt":False
            },
            "order": consolidatedOrder,
            "lineItemDiscounts":self.lineItemDiscounts(raw)
        }
        
        orderRecord.consolidated = input
        orderRecord.save()
        return input
    def calculateFulfillment(self,order:SearchableDict):
        
        totalItemsCount = functools.reduce(
            lambda a,b:a+b.get("quantity"),
            order.search("lineItems.nodes",[]),
            0
        )
        successfulfullfillmentsCount = 0
        for fulfillment in [SearchableDict(x) for x in order.get("fulfillments",[])]:
            
            
            if fulfillment.get("status")=="SUCCESS":
                successfulfullfillmentsCount = successfulfullfillmentsCount + functools.reduce(
                    lambda a,b:a+b.get("quantity"),
                    fulfillment.search("fulfillmentLineItems.nodes",[]),
                    0
                )
        if successfulfullfillmentsCount>=totalItemsCount:
            return "FULFILLED"
        elif successfulfullfillmentsCount<1:
            return "RESTOCKED"
        elif successfulfullfillmentsCount<totalItemsCount:
            return "PARTIAL"
        
        return "RESTOCKED"
        
        """if len(fulfillments)<1:
            return "RESTOCKED"
        
        def fulfillmentStatusCount(fulfillments,status):
            return functools.reduce(
                lambda a,b: a+1 if b.get("status")==status else a+0,
                fulfillments,
                0
            )
        totalItems = len(fulfillments)
        fulfilledItems = fulfillmentStatusCount(fulfillments,"SUCCESS")
        sys.stderr.write(f"{totalItems}/{fulfilledItems} : ")
        if fulfilledItems==totalItems:
            return "FULFILLED"
        elif  fulfilledItems>0:
            return "PARTIAL"
        elif fulfilledItems==0:
            return 'RESTOCKED"""
    def lineItemDiscounts(self,order:SearchableDict):
        discounts = {}
        hasDiscounts = False
        lineItem:SearchableDict
        for index,lineItem in enumerate(order.getAsSearchable("lineItems.nodes")):
            applications = []
            for discountAllocation in lineItem.getAsSearchable("discountAllocations"):
                discountAllocation:SearchableDict
                if discountAllocation.search("discountApplication.targetSelection") == "ALL":
                    continue
                applications.append(discountAllocation.get("discountApplication"))
                
            if len(applications):
                discounts[str(index)] = applications
                hasDiscounts = True
        if hasDiscounts:
            return discounts
        return None
                    
                    
                
            
        
    def processLineItems(self,lineItems:List[SearchableDict]):
        processedLineItems = []
        for lineItem in lineItems:
            
            processedLineItem = {
                "quantity":lineItem.get("quantity"),
                #"taxLines":lineItem.get("taxLines"),
                #"discountAllocations":lineItem.get("discountAllocations",[])
            }
            shopifyVariantId = None
            lineItemVariantId =  lineItem.search("variant.inventoryItem.variant.id")
            if lineItemVariantId is not None:
                shopifyVariantId = ShopifyOperation.lookupItemId(lineItem.search("variant.inventoryItem.variant.id"))
                
            if shopifyVariantId is not None:
                processedLineItem["variantId"] = shopifyVariantId
            else:
                
                processedLineItem["sku"]=lineItem.get("sku")
                processedLineItem["title"]=lineItem.get("title")
                processedLineItem["vendor"]=lineItem.get("vendor")
                processedLineItem["giftCard"]=lineItem.get("isGiftCard")[0] if isinstance(lineItem.get("giftCard"),list) else lineItem.get("giftCard")
                processedLineItem["requiresShipping"]=lineItem.get("requiresShipping")[0] if isinstance(lineItem.get("requiresShipping"),list) else lineItem.get("requiresShipping")
                
                
                processedLineItem["priceSet"]={
                    "shopMoney":{                        
                        "currencyCode":lineItem.search("priceSet.shopMoney.currencyCode"),
                        "amount":float(lineItem.search("priceSet.shopMoney.amount"))/lineItem.get("quantity")
                    }
                }
                                                         
            processedLineItems.append(processedLineItem)
        return processedLineItems
        
    def customAttributesToProperties(self,attributes):
        return [
            {"name":attribute.get("key"),"value":attribute.get("value")}
            for attribute in attributes
        ]
    
            
            
        
class ShopifyOrderCreator(ShopifyCreator):
    def recordType(self):
        return "order"
    def sortOrder(self):
        return "desc"
        
    def processRecord(self,order:Record,reconsolidate=True):
        
        recordLookup = super().processRecord(order)
        consolidated = order.consolidated
        if reconsolidate:
            consolidated = ShopifyOrderConsolidator(processor=self.processor).run(orderId=order.externalId)
        
        
        
        shopifyOrder = GraphQL().run(
            """
            mutation OrderCreate($order: OrderCreateOrderInput!,$options: OrderCreateOptionsInput) {
                orderCreate(order: $order, options: $options) {
                      userErrors {
                        field
                        code
                        message
                    }
                    order {
                        id
                        closed
                        lineItems(first:50) {
                            nodes {
                                id
                            }
                            
                        }
                    }
                }
            }
            """,
            {
                "options":consolidated.get("options"),
                "order":consolidated.get("order")
            },
            consolidated
        )
        try:
            orderId = shopifyOrder.search("data.orderCreate.order.id")
        except:
            print("order is null; timed out")
            return
        if orderId is None:
            shopifyOrder.dump()
            sys.exit()

        
        
        
        recordLookup.shopifyId  = orderId
        recordLookup.save()
        order.shopifyId = orderId
        order.save()
        
        hasDiscounts = False
        print(f"created order {orderId}",flush=True)
        
        
        if consolidated.get("lineItemDiscounts") is None:
            return
        lineItemDiscounts:dict = consolidated.get("lineItemDiscounts")
        
        shopifyOrderEdit = self.orderEditBegin(orderId)
        calculatedOrderId = shopifyOrderEdit.search("data.orderEditBegin.calculatedOrder.id")
        if calculatedOrderId is None:
        
            sys.exit()
            
        
        calculatedOrderLineItems = shopifyOrderEdit.nodes("data.orderEditBegin.calculatedOrder.lineItems")
        for index,lineItem in enumerate(calculatedOrderLineItems):
            lineItemDiscountApplications = lineItemDiscounts.get(str(index))
            if len(lineItemDiscountApplications)>0:
                for lineItemDiscountApplication in [SearchableDict(x) for x in lineItemDiscountApplications]:
                    self.addLineItemDiscount(
                        calculatedOrderId,
                        lineItem.get("id"),
                        lineItemDiscountApplication
                    )
        self.orderEditClose(calculatedOrderId)
            
        
        
            
        
    def orderEditBegin(self,orderId):
        return GraphQL().run(
            """
                mutation orderEditBegin($id: ID!) {
                    orderEditBegin(id: $id) {
                        calculatedOrder {
                            id
                            lineItems(first:100) {
                                nodes {
                                    id
                                }
                            }
                        }
                        userErrors {
                        field
                        message
                        }
                    }
                }
            """,
            {"id":orderId}
        )
        
    def addLineItemDiscount(self,orderEditId,orderEditLineItemId,discount):
        
        discountDetails = None
        if discount.search("value.percentage"):
            discount = {
                "percentValue":discount.search("value.percentage")
            }
        else:
            discount = {
                "fixedValue":{
                    "amount":discount.search("value.amount"),
                    "currencyCode":"USD"
                }
            }
        addDiscount = GraphQL().run(
            """
            mutation orderEditAddLineItemDiscount($id: ID!, $lineItemId: ID!, $discount: OrderEditAppliedDiscountInput!) {
                orderEditAddLineItemDiscount(id: $id, lineItemId: $lineItemId, discount: $discount) {
                    calculatedOrder {
                        id
                    }
                    calculatedLineItem {
                        id
                        calculatedDiscountAllocations {
                            discountApplication {
                                id
                            }
                        }
                    }
                    addedDiscountStagedChange {
                        id
                        description
                        value {
                            __typename
                                ... on PricingPercentageValue {
                                percentage
                            }
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
                "id":orderEditId,
                "lineItemId":orderEditLineItemId,
                "discount":discount
            }
        )
        
        
        pass
    def orderEditClose(self,orderId):
        return GraphQL().run(
            """
            mutation orderEditCommit($id: ID!,$staffNote:String) {
                orderEditCommit(id: $id,staffNote:$staffNote) {
                    order {
                        id
                        currentSubtotalPriceSet {
                            presentmentMoney {
                                amount
                            }
                            shopMoney {
                                amount
                            }
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
                "id":orderId,
                "notifyCustomer": False,
                "staffNote": "Added Bundle Items"
            }
        )
        
        
class ShopifyOrderDeleter(ShopifyDeleter):
    
    
    def run(self,record:Record=None,all=False):
        if isinstance(record,SearchableDict):
            self.delete(record.get("id"))
        if all:
            for record in Record.objects.filter(recordType="order").filter(~Q(shopifyId="")).all():
                if record.shopifyId!="":
                    self.delete(record.shopifyId)
        else:
            self.delete(record.shopifyId)
            
            
    def delete(self,shopifyId):
        print(shopifyId,flush=True)
        ret =  GraphQL().run(
            """
            mutation OrderDelete($orderId: ID!) {
                orderDelete(orderId: $orderId) {
                    deletedId
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }
            """,
            {"orderId":shopifyId}
        )
        deletedOrderId = ret.search("data.orderDelete.deletedId")
        if deletedOrderId is not None:
            record = Record.objects.get(shopifyId=shopifyId)
            recordLookup = RecordLookup.objects.get(externalId=record.externalId)
            for toClear in [record,recordLookup]:
                toClear.shopifyId = ""
                toClear.numericShopifyId = None
                toClear.save()
        ret.dump()