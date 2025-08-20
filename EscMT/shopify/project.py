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
        return []
    
    @staticmethod
    def additionalProductMetafields(product):
        return []