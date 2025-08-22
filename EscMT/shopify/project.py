from EscMT.base import *
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit,SearchableDict
from EscMT.models import *
from EscMT.graphQL import *
from .operations import *
from django.db.models import Q

class FringeProcessor(ProjectCreatorOptions):
    def variantMetafields(self,variant:SearchableDict,metafields):
        return [
            {"namespace":"data",
             "key":"petshop_variant_id",
             "type":"number_integer",
             "value":variant.get("id").split("/")[-1]
             }
        ]+metafields
    
    
    def productMetafields(self,product:SearchableDict,metafields):
        return [
            {
                "namespace":"data",
                "key":"petshop_product_id",
                "type":"number_integer",
                "value":product.get("id").split("/")[-1]
            }
        ]+metafields
    def orderMetafields(self, order,metafields):
       return [ {
                "namespace":"data",
                "key":"petshop_order_id",
                "type":"single_line_text_field",
                "value":order.get("id").split("/")[-1]
            }
       ]+metafields
       
    def additionalCustomerMetafields(self, record):
        return super().additionalCustomerMetafields(record)
    
    def finalizeConsolidation(self, consolidated):
        return consolidated
    
    def tags(self,tags):
        return tags
    
class UchOrderQueryGenerator(ShopifyQueryGenerator):
    def searchQuery(self):
        query = ""
        subquery = " processed_at:>=2020-01-01 AND processed_at:<=2022-01-01 AND fulfillment_status:unfulfilled AND financial_status:paid "
        if query!="":
            query = f"{query} AND {subquery}"
        else:
            query = subquery

        print(query)
        return query