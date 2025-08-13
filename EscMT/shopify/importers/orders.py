from EscMT.base import *
from .base import ShopifyImporter
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *

class ShopifyOrderImporter(ShopifyImporter):
    def setGql(self):
        return Order()
    
        