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

def close_db(signal,frame):
    sys.exit()
    
signal.signal(signal.SIGINT,close_db)   
signal.signal(signal.SIGTERM,close_db)

class ShopifyOrderImporter(ShopifyImporter):
    def setGql(self):
        return Order()
    def recordType(self):
        return "order"
    def run(self):
        processedCount = 0
        for orderGroup in self.gql.iterable(
            """
            query getOrders($after:String,$query:String) {
                orders(after:$after,query:$query,first:50) {
                    nodes {                        
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
                        fulfillments {
                            status
                            createdAt
                            displayStatus
                            fulfillmentLineItems(first:10) {
                                nodes {
                                    quantity
                                    lineItem {
                                        quantity
                                    }
                                }
                            }
                        }
                        id
                        lineItems(first:20) {
                            nodes {
                                customAttributes {
                                    key
                                    value
                                }
                                discountAllocations {
                                    discountApplication{
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
            
            order:GqlReturn
            for order in orderGroup:
                self.processRecord(order)
            processedCount = processedCount + 1
            print(f"Processed group {processedCount}")
            sys.exit()
                
    def processRecord(self, order):
        
        try:
            overageItems:dict = self.loadApiOverageItems(order.get("id")).search("data.order")
            for overageItemId,overageItemValue in overageItems.items():
                order.set(overageItemId,overageItemValue)
        except:
            print(f"no API overage items for {order.get("id")}")
            
        self.createRecords(order.get("name"),order)
        print(f"order {order.get('id')}")
        ShopifyOrderConsolidator().run(order=order)
        
                    
                    
    def loadApiOverageItems(self,orderId)->GqlReturn:
        return self.gql.run(
            """
            query getOrder($orderId:ID!) {
                order(id:$orderId) {
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
                            priceSet: originalPriceSet {
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
                }
            }
            """,
            {
                "orderId":orderId
            }
        )
        
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
                "customer":{
                    "toAssociate":{
                        "id":raw.search("customer.id")
                    }
                },
                "email":raw.get("email"),
                "fulfillmentStatus":self.calculateFulfillment(raw),
                "customAttributes":raw.get("customAttributes"),
                "lineItems":self.processLineItems([SearchableDict(lineItem) for lineItem in raw.search("lineItems.nodes")]),
                "metafields":raw.search("metafields.nodes"),
                "name":f"PS-{raw.get('number')}",
                "processedAt":raw.get("processedAt"),
                "shippingAddress":raw.get("shippingAddress"),
                "shippingLines":raw.search("shippingLines.nodes"),
                "tags":["ps_prder"]+raw.get("tags"),
                "taxesIncluded":raw.get("taxesIncluded"),
                "taxLines":raw.get("taxLines"),
                "transactions":raw.get("transactions"),    
            }
        discountCodes = raw.get("discountCodes")
        discountCodeInput = None
        hasSpecificLineItemDiscounts = False
        if len(discountCodes)>0:
            discountCodeInput = {}
            for index,application in enumerate([SearchableDict(x) for x in raw.search("discountApplications.nodes")]):
                discountCode = discountCodes[index]
                if application.get("targetType")=="SHIPPING_LINE":
                    discountCodeInput["freeShippingDiscountCode"] = discountCode
                    continue
                if application.search("value.percentage") is not None:
                    discountCodeInput["itemPercentageDiscountCode"] = {
                        "percentage":application.search("value.percentage"),
                        "code":discountCodes[index]
                    }
                else:
                    discountCodeInput["itemPercentageDiscountCode"] = {
                        "amountSet":application.search("value"),
                        "code":discountCodes[index]
                    }
                
        if discountCodeInput is not None:
            consolidatedOrder["discountCode"] = discountCodeInput
        
                
                
        
        input = {
            "options":{
                "inventoryBehaviour":"BYPASS",
                "sendFulfillmentReceipt":False,
                "sendReceipt":False
            },
            "order": consolidatedOrder
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
        
    def processLineItems(self,lineItems:List[SearchableDict]):
        processedLineItems = []
        for lineItem in lineItems:
            if len(lineItem.get("discountAllocations",[]))>0:
                print("ORDER HAS DISCOUNTS")
            processedLineItem = {
                "quantity":lineItem.get("quantity"),
                "properties":self.customAttributesToProperties(lineItem.get("customAttributes")),
                "giftCard":lineItem.get("isGiftCard"),
                "requiresShipping":lineItem.get("requiresShipping"),
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
        
    def processRecord(self,order:Record):
        
        recordLookup = super().processRecord(order)
        consolidated = ShopifyOrderConsolidator().run(orderId=order.externalId)
        originalLineItems = consolidated["order"].get("lineItems")
        print(consolidated.get("order"))
        consolidated["order"]["lineItems"] = [stripDict(x,["discountAllocations"]) for x in consolidated["order"].get("lineItems")]
        print("creating order")
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
                                discountAllocations {
                                    allocatedAmountSet {
                                        shopMoney {
                                            amount
                                        }
                                    }
                                }
                            }
                            
                        }
                    }
                }
            }
            """,
            consolidated
        )
        orderId = shopifyOrder.search("data.orderCreate.order.id")
        if orderId is None:
            shopifyOrder.dump()
            sys.exit()

        
        
        
        recordLookup.shopifyId  = orderId
        recordLookup.save()
        order.shopifyId = orderId
        order.save()
        
        hasDiscounts = False
        lineItems = [SearchableDict(x) for x in originalLineItems]
        for lineItem in lineItems:
            lineItem.dump()
            if len(lineItem.get("discountAllocations",[]))>0:
                hasDiscounts = True
                break
        time.sleep(5)
        return
        if not hasDiscounts:
            return
        print("hasDiscounts")
        shopifyOrderEdit = self.orderEditBegin(orderId)
        calculatedOrderId = shopifyOrderEdit.search("data.orderEditBegin.calculatedOrder.id")
        if calculatedOrderId is None:
            shopifyOrderEdit.dump()
            sys.exit()
            
        shopifyOrderEdit.dump()
        calculatedOrderLineItems = shopifyOrderEdit.nodes("data.orderEditBegin.calculatedOrder.lineItems")
        for index,lineItem in enumerate(lineItems):
            calculatedOrderLineItem = calculatedOrderLineItems[index]
            if len(lineItem.get("discountAllocations",[]))>0:
                for lineItemDiscount in [SearchableDict(x) for x in lineItem.get("discountAllocations")]:
                    self.addLineItemDiscount(
                        calculatedOrderId,
                        calculatedOrderLineItem.get("id"),
                        lineItemDiscount.getAsSearchable("discountApplication")
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
        print("adding line discount")
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
        addDiscount.dump()
        
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
        if all:
            for record in Record.objects.filter(recordType="order").all():
                if record.shopifyId!="":
                    recordLookup = RecordLookup.objects.get(shopifyId=record.shopifyId)
                    
                    self.delete(record.shopifyId)
                    record.shopifyId = ""
                    record.save()
                    recordLookup.save()
        else:
            self.delete(record.shopifyId)
            
            
    def delete(self,shopifyId):
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
        ret.dump()