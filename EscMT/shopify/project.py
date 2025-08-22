from EscMT.base import *
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit,SearchableDict
from EscMT.models import *
from EscMT.graphQL import *
from .operations import *
from django.db.models import Q

class FringeProcessor(ProjectCreatorOptions):
    def additionalVariantMetafields(self,variant:SearchableDict):
        return [
            {"namespace":"data",
             "key":"petshop_variant_id",
             "type":"number_integer",
             "value":variant.get("id").split("/")[-1]
             }
        ]
    
    
    def additionalProductMetafields(self,product:SearchableDict):
        return [
            {
                "namespace":"data",
                "key":"petshop_product_id",
                "type":"number_integer",
                "value":product.get("id").split("/")[-1]
            }
        ]
    def additionalOrderMetafields(self, order):
       return [ {
                "namespace":"data",
                "key":"petshop_order_id",
                "type":"single_line_text_field",
                "value":order.get("id").split("/")[-1]
            }
       ]
        
    