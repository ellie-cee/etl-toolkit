from EscMT.base import *
from .base import ShopifyImporter
import mysql.connector
import os
from EscMT.misc import jsonify,loadProfiles,shopifyInit
from EscMT.models import *
from EscMT.graphQL import *
from django.forms.models import model_to_dict

class ShopifyMediaCreator(ShopifyImporter):
    def setGql(self):
        return Files()
    
    def run(self):
        gql = Files()
        for record in RecordLookup.objects.filter(recordType="media",shopifyId="").all():
        #for record in [RecordLookup.objects.filter(recordType="media",shopifyId="").first()]:
            uploaded = gql.upload(
                record.supplementary,
                "altText"
            )
            imageId = uploaded.search("data.fileCreate.files[0].id")
            if (imageId) is None:
                continue
            record.shopifyId = imageId
            record.save()
            print(f"uploaded {record.recordKey}")
            