from ..base import *
import mysql.connector
import os
from ..misc import jsonify
from ..models import *

class MagentoClient:
    def __init__(self):
        self.db = mysql.connector.connect(
	        host=os.environ.get("MAGENTO_DB_HOST"),
	        user=os.environ.get("MAGENTO_DB_USER"),
	        password=os.environ.get("MAGENTO_DB_PASS"),
	        database=os.environ.get("MAGENTO_DB_NAME")
        )
    def cursor(self):
        return self.db.cursor(dictionary=True,buffered=True)
class MagentoIterable(MagentoClient):
    def __init__(self):
        super().__init__()
        self.query = None
    def __iter__(self):
        return self    
    def __next__(self):
        if self.query is None:
            raise StopIteration
        ret = self.query.fetchone()
        if ret is None:
            raise StopIteration
        return ret
class MagentoOrders(MagentoIterable):
    def __init__(self,startAt=0,where=None,postfix=""):
        super().__init__()
        whereClause = f"where a.increment_id>{startAt}"
        if where is not None:
            whereClause= f"{whereClause} and {where}"
        
        
        self.query = self.cursor()
        self.query.execute(
            f"""
                select a.entity_id as rowId, a.increment_id as externalId, a.state as status,a.created_at as createdAt,
                a.subtotal,a.base_total_refunded as refundedAmount,a.base_total_canceled as canceledAmount,
                a.base_total_paid as paidAmount,a.payment_authorization_amount as authorizedAmount,
                a.shipping_description as shippingLabel,a.shipping_method as shippingMethod,
                a.shipping_amount as shippingAmount, a.shipping_tax_amount as shippingTax,
                a.tax_amount as taxAmount,a.fee,a.created_at as createdAt, a.updated_at as updatedAt,
                a.billing_address_id as billingAddressId,a.shipping_address_id as shippingAddressId,
                a.customer_email as email, a.customer_firstname as customerFirstName,
                a.customer_lastname as customerLastName,a.customer_id as customerId,
                b.track_number as trackingNumber,a.coupon_code as discountCode,
                a.base_adjustment_negative as adjustmentAmount
                from sales_order a
                left join sales_shipment_track b on a.entity_id=b.parent_id
                {whereClause} {postfix}"""
        )
    def __next__(self):
        order = self.query.fetchone()
        if order is None:
            raise StopIteration
        orderId = order.get("rowId")
        order["billingAddress"] = self.getAddress(order.get("billingAddressId"))
        if order.get("shippingAddressId") is not None:
            order["shippingAddress"] = self.getAddress(order.get("shippingAddressId"))
        else:
            order["shippingAddress"] = order.get("billingAddress")
            
        order["lineItems"] = self.getOrderLineItems(orderId)
        order["paymentHistory"] = self.getPayments(orderId)
        order["statusHistory"] = self.getStatuses(orderId)
        order["taxes"] = self.getTaxes(orderId)
        return SearchableDict(jsonify(order))
    def query(self,query):
        cursor = self.cursor()
        cursor.execute(query)
    def getAddress(self,addressId):
        cursor = self.cursor()
        cursor.execute(
            f"""
            select a.firstname as firstName, a.lastname as lastName,
            a.city, a.postcode as zip, a.street as address1, region.code as provinceCode,
            region.country_id as countryCode,a.telephone as phone
            
            from sales_order_address a
            join directory_country_region region on region.region_id=a.region_id
            where entity_id = {addressId} limit 1
            """
        )
        ret = cursor.fetchone()
        cursor.close()
        return ret
    def getOrderLineItems(self,orderId):
        cursor = self.cursor()
        cursor.execute(
            f"""
            select a.sku,a.name as title,a.base_price as price,
            cast(a.qty_ordered as unsigned) as quantity,
            cast(a.qty_canceled as unsigned) as canceled,
            cast(a.qty_refunded as unsigned) as refunded,
            a.discount_percent as discountPercent, a.discount_amount as discountAmount
            
            
            from sales_order_item a
            where a.order_id = {orderId} and a.parent_item_id is null
            """
        )
        ret =  cursor.fetchall()
        cursor.close()
        return ret if ret is not None else []
    def getPayments(self,orderId):
        cursor = self.cursor()
        cursor.execute(
            f"""
            select amount_refunded as refunded, amount_canceled as canceled, amount_paid as paid,
            method as gateway, additional_information as receiptJson
            from sales_order_payment
            where parent_id = {orderId}
            """
        )
        ret = cursor.fetchall()
        cursor.close()
        return ret if ret is not None else []
    def getStatuses(self,orderId):
        cursor = self.cursor()
        cursor.execute(
            f"""
            select status,comment,created_at as createdAt
            from sales_order_status_history
            where parent_id = {orderId}
            """
        )
        ret = cursor.fetchall()
        cursor.close()
        return ret if ret is not None else []
    def getTaxes(self,orderId):
        cursor = self.cursor()
        cursor.execute(
            f"""
            select a.code,a.percent,a.amount as totalAmount,sum(b.amount) as amount,
            b.taxable_item_type as type 
            from sales_order_tax a join sales_order_tax_item b on a.tax_id=b.tax_id
            where a.order_id={orderId} and b.amount>0 group by type
            """
        )
        ret = cursor.fetchall()
        cursor.close()
        return ret if ret is not None else []
    
class ShopifyConsolidator:
    pass