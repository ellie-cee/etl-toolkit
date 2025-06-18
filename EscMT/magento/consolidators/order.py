
from EscMT.base import *
import mysql.connector
import os
from EscMT.misc import jsonify
from EscMT.models import *
from functools import reduce



class MagentoOrderConsolidator:
    def __init__(self):
        pass
    def paymentStatus(self,order:SearchableDict):
        amountPaid = self.amount(order.get("paidAmount"))
        amountRefunded = self.amount(order.get("refundedAmount"))+self.amount(order.get("adjustmentAmount"))
        amountCanceled = self.amount(order.get("canceledAmount"))
        amountAuthorized = self.amount(order.get("authorizedAmount"))
        subtotal = self.amount(order.get("subtotal"))
        if amountPaid>0:
                if amountRefunded>0:
                    if amountRefunded<subtotal:
                        return "PARTIAL_REFUND"
                    else:
                        return "REFUNDED"
                else:
                    return "PAID"
        return "CANCELLED"
        
    def getFulfillmentStatus(self,order:SearchableDict):
        fulfillmentStatuses = {
            "closed":"FULFILLED",
            "complete":"FULFILLED",
            "canceled":"RESTOCKED",
        }
        return fulfillmentStatuses.get(order.get("status"))
        
    def process(self,order:SearchableDict,defaulLocationId=None):
        customer = None
        try:
            customer = CustomerLookup.objects.get(email=order.get("email"))
        except:
            pass
       
        
        lineItems = []
        discounts = 0
        
        discounts = reduce(lambda a,b:a+self.amount(b.get("discountAmount",0)),order.get("lineItems",[]),0)
        customerData:dict = None
        if customer is not None:
            customerData = {
                "toAssociate":{
                    "id":customer.customerId
                }
            }
        else:
            addresses = []
            if order.get("billingAddress"):
               addresses.append(order.get("billingAddress"))
            if order.get("shippingAddress"):
                addresses.append(order.get("shippingAddress"))
                
            
            customerData = {
                "toUpsert": {
                    #"addresses":self.upsetAddresses(addresses),
                    "email":order.get("email")
                }
            }
            if order.get("firstName","")!="":
                    customerData["toUpsert"]["firstName"] = order.get("firstName","")
                    customerData["toUpsert"]["firstName"] = order.get("lastName","")
                    
        for lineItem in order.get("lineItems",[]):
            productInfo = ProductInfo.objects.filter(SKU=lineItem.get("sku")).first()
            if productInfo is None:
                lineItems.append(
                    {
                        "sku":lineItem.get("sku"),
                        "quantity":lineItem.get("quantity"),
                        "title":lineItem.get("title"),
                        "priceSet":{
                            "shopMoney":{
                                "currencyCode":"USD",
                                "amount":lineItem.get("price",0.0)
                            }
                        },
                    }
                )
            else:
                lineItems.append(
                    {
                        "variantId":productInfo.variantId,
                        "quantity":lineItem.get("quantity"),
                        "priceSet":{
                            "shopMoney":{
                                "currencyCode":"USD",
                                "amount":lineItem.get("price",0.0)
                            }
                        }
                    }
                )
        if order.get("fee",0)>0:
            lineItems.append(
                {
                    "title":"Shipping Protection",
                    "sku":"SHIP-PROTECT",
                    "requiresShipping":False,
                    "quantity":1,
                    "priceSet":{
                        "shopMoney":{
                            "currencyCode":"USD",
                            "amount":order.get("fee")
                        }
                    },
                }
            )
        
        fulfillmentStatuses = {
            "closed":"FULFILLED",
            "complete":"FULFILLED",
            "canceled":"RESTOCKED",
        }
        fulfillmentStatus = fulfillmentStatuses.get(order.get("status"))
        fulfillmentInput = {
            "locationId":defaulLocationId
        }
        if order.get("trackingNumber") is not None:
            fulfillmentInput["trackingNumber"] = order.get("trackingNumber")
        shippingLines = []
        shippingAmount = self.amount(order.get("shippingAmount"))
        shippingLine = {
            "code":order.get("shippingMethod","shipping-standard"),
            "title":order.get("shippingLabel","Standard Shipping"),
            "priceSet":{
                "shopMoney":{
                    "currencyCode":"USD",
                    "amount":shippingAmount
                }
            }
        }
        shippingTaxAmount = self.amount(order.get("shippingTax"))
        if shippingTaxAmount>0:
            shippingLine["taxLines"] = [
                {
                    "priceSet":{
                        "shopMoney":{
                            "amount":shippingTaxAmount,
                            "currencyCode":"USD"
                        }
                    },
                    "rate":shippingTaxAmount/shippingAmount,
                    "title":"Shipping Tax"
                    
                }
            ]
        shippingLines.append(shippingLine)
        
        
        transactions = []
        amountPaid = self.amount(order.get("paidAmount"))
        amountRefunded = self.amount(order.get("refundedAmount"))+self.amount(order.get("adjustmentAmount"))
        amountCanceled = self.amount(order.get("canceledAmount"))
        amountAuthorized = self.amount(order.get("authorizedAmount"))
        
        transactionStatus = {
            "canceled":"SUCCESS",
            "complete":"SUCCESS",
            "closed":"SUCCESS",
            "new":"PENDING",
            "payment_review":"PENDING",
            "pending_payment":"AWAITING_RESPONSE",
            "processing":"PENDING"
        }.get(order.get("status"))
        
        subtotal = self.amount(order.get("subtotal"))
        if amountAuthorized:
            transactions.append(self.transaction("AUTHORIZED",amountPaid))
            if amountPaid>0:
                transactions.append(self.transaction("CAPTURE",amountPaid))
        else:
            if amountPaid>0:
                transactions.append(self.transaction("SALE",amountPaid))
                if amountRefunded>0:
                    transactions.append(self.transaction("REFUND",amountRefunded))
                if amountRefunded<subtotal:
                    fulfillment = ""
            elif amountCanceled>0:
                transactions.append(self.transaction("VOID",amountRefunded))
                
        
        
        
        
        orderInput = {
            "customer":customerData,
            
            "processedAt":self.utc8601Date(order.get("createdAt")),
            "customAttributes":[
                {"key":"magento_order_id","value":str(order.get("externalId"))},
                {"key":"original_order_date","value":order.get("createdAt")}
            ], 
            "lineItems":lineItems,
            "transactions":transactions,
            "tags":["imported-from-magento"],
            "sourceIdentifier":str(order.get("externalId")),
        }
        if True:
            orderInput["taxLines"]=[
                {
                    "title":x.get('code'),
                    "rate":x.get("percent")/100 if x.get("percent") is not None else 0,
                    "priceSet":{
                        "shopMoney":{
                            "currencyCode":"USD",
                            "amount":x.get('amount')
                        }
                    }
                }
                for x in filter(lambda x:x.get("type")=="product",order.get("taxes",[]))
            ]
        if order.get("billingAddress"):
         orderInput["billingAddress"]=order.get("billingAddress")
        if order.get("shippingAddress"):
         orderInput["shippingAddress"]=order.get("shippingAddress")
            
        if fulfillmentInput is not None and False:
            orderInput["fulfillment"] = fulfillmentInput
        if fulfillmentStatus is not None:
            orderInput["fulfillmentStatus"] = fulfillmentStatus
            
        if len(shippingLines)>0:
            orderInput["shippingLines"] = shippingLines
        
            
        if discounts>0:
            orderInput["discountCode"] = {
            "itemFixedDiscountCode":{
                "code":f"Discount{order.get('externalId')}" if order.get("discountCode") is None else order.get("discountCode"),
                "amountSet":{
                    "shopMoney":{
                        "currencyCode":"USD",
                        "amount":discounts,
                    }
                }
            }
            }
        
        return {
            "options":{
                "inventoryBehaviour":"BYPASS",
                "sendFulfillmentReceipt":False,
                "sendReceipt":False
            },
            "order":orderInput
        }
    def transaction(self,type,amount):
        return {
            "amountSet":{
                "shopMoney":{
                    "amount":amount,
                    "currencyCode":"USD"
                }
            },
            "kind":type,
            
        }
    def amount(self,amount):
        if amount is None:
            return 0.0
        return amount
    def utc8601Date(self,date:str):
        parts = date.split(" ")
        return f"{parts[0]}T{parts[1]}Z"
    def upsertAddresses(self,addresses):
        
        pass

class MagentoCreateDraftOrder:

    def __init__(self):
        pass
    def run(self,order:SearchableDict,defaulLocationId=None):
        customer = None
        try:
            customer = CustomerLookup.objects.get(email=order.get("email"))
        except:
            pass
       
        
        lineItems = []
        for lineItem in order.get("lineItems",[]):
            productInfo = ProductInfo.objects.filter(SKU=lineItem.get("sku")).first()
            if productInfo is None:
                lineItems.append(
                    {
                        "sku":lineItem.get("sku"),
                        "quantity":lineItem.get("quantity"),
                        "title":lineItem.get("title"),
                        "originalUnitPriceWithCurrency":{
                            "currencyCode":"USD",
                            "amount":lineItem.get("price",0.0)
                        }
                    }
                )
            else:
                lineItems.append(
                    {
                        "variantId":productInfo.variantId,
                        "quantity":lineItem.get("quantity"),
                        "priceOverride":{
                            "currencyCode":"USD",
                            "amount":lineItem.get("price",0.0)
                        }
                    }
                )
        if order.get("fee",0)>0:
            lineItems.append(
                {
                    "title":"Shipping Protection",
                    "sku":"SHIP-PROTECT",
                    "requiresShipping":False,
                    "quantity":1,
                    "originalUnitPriceWithCurrency":{
                        "currencyCode":"USD",
                        "amount":order.get("fee")
                    }
                },
            )
        discounts = reduce(lambda a,b:a+b.get("discountAmount",0) if b.get("discountAmount",0) is not None else 0,order.get("lineItems",[]),0)
        customerData:dict = None
        purchasingEntity = None
        if customer is not None:
            purchasingEntity = {
                "customerId":customer.customerId
            }
        shippingLines = []
        shippingAmount = self.amount(order.get("shippingAmount"))
        shippingLine = {
            "shippingRateHandle":order.get("shippingMethod","shipping-standard"),
            "title":order.get("shippingLabel","Standard Shipping"), 
            "priceWithCurrency":{
                "currencyCode":"USD",
                "amount":shippingAmount
            }
        }
        shippingTaxAmount = self.amount(order.get("shippingTax"))
        if shippingTaxAmount>0:
            shippingLine["taxLines"] = [
                {
                    "priceSet":{
                        "shopMoney":{
                            "amount":shippingTaxAmount,
                            "currencyCode":"USD"
                        }
                    },
                    "rate":shippingTaxAmount/shippingAmount,
                    "title":"Shipping Tax"
                    
                }
            ]
        
        
        
        transactions = []
        amountPaid = self.amount(order.get("paidAmount"))
        amountRefunded = self.amount(order.get("refundedAmount"))+self.amount(order.get("adjustmentAmount"))
        amountCanceled = self.amount(order.get("canceledAmount"))
        amountAuthorized = self.amount(order.get("authorizedAmount"))
        
        transactionStatus = {
            "canceled":"SUCCESS",
            "complete":"SUCCESS",
            "closed":"SUCCESS",
            "new":"PENDING",
            "payment_review":"PENDING",
            "pending_payment":"AWAITING_RESPONSE",
            "processing":"PENDING"
        }.get(order.get("status"))
        
        orderInput = {
            
            
            "customAttributes":[
                {"key":"magento_order_id","value":order.get("externalId")},
                {"key":"original_order_date","value":order.get("createdAt")}
            ], 
            "lineItems":lineItems,
            "tags":["imported-from-magento"],
            "email":order.get("email")
        }
        if order.get("billingAddress"):
         orderInput["billingAddress"]=order.get("billingAddress")
        if order.get("shippingAddress"):
         orderInput["shippingAddress"]=order.get("shippingAddress")
        if purchasingEntity:
            orderInput["purchasingEntity"] = purchasingEntity

        if len(shippingLines)>0:
            orderInput["shippingLine"] = shippingLine
        
            
        if discounts>0:
            orderInput["appliedDiscount"] = {
                "amountWithCurrency":{
                    "currencyCode":"USD",
                    "amount":discounts,    
                },
                "title":"Discounts",
                "value":discounts,
                "valueType":"FIXED_AMOUNT"
            }
        
        return {
            "input":orderInput
        }
    def amount(self,amount):
        if amount is None:
            return 0.0
        return amount
