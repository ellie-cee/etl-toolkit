from EscMT.base import *
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit,SearchableDict
from EscMT.models import *
from EscMT.graphQL import *
from django.db.models import Q

class ProjectSecific:
    
    @staticmethod
    def additionalVariantMetafields(variant):
        return [{"namespace":"data","key":"petshop_variant_id","type":"number_integer","value":variant.get("id").split("/")[-1]}]
    
    @staticmethod
    def additionalProductMetafields(product):
        return [{"namespace":"data","key":"petshop_product_id","type":"number_integer","value":product.get("id").split("/")[-1]}]
    
    @staticmethod
    def heynow():
        pass