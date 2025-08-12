import json
import os
import sys
import traceback
import signal
try:
    from .misc import SearchableDict
except:
    from misc import SearchableDict
    
import datetime
import django
from django.conf import settings
from django.db import models,connections,close_old_connections

from django.db.models import Model

"""
_database = MySQLDatabase(
    os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    host=os.environ.get("DB_HOST")
)"""



class Database:
    def __init__(self, engine='django.db.backends.sqlite3', name=None, user=None, password=None, host=None, port=None):
        self.Model = None

        
        databases = {
            'default': {
                'ENGINE': engine,
                'NAME': name,
                'USER': user,
                'PASSWORD': password,
                'HOST': host,
                'PORT': port,
                'APP_LABEL': 'isolated',
            }
        }
        settings.configure(DATABASES=databases,USE_TZ=False)
        django.setup()
        class CustomBaseModel(Model):
            class Meta:
                app_label = 'isolated'
                abstract = True

        self.Model = CustomBaseModel

    def create_table(self, model):
        with connections['default'].schema_editor() as schema_editor:
            if model._meta.db_table not in connections['default'].introspection.table_names():
                schema_editor.create_model(model)

    # Update table if you added fields (doesn't drop fields as far as i know, which i was too afraid to implement)
    def update_table(self, model):
        with connections['default'].schema_editor() as schema_editor:
            # Check if the table exists
            if model._meta.db_table in connections['default'].introspection.table_names():
                # Get the current columns in the table
                current_columns = [field.column for field in model._meta.fields]

                # Get the database columns
                database_columns = connections['default'].introspection.get_table_description(connections['default'].cursor(), model._meta.db_table)
                database_column_names = [column.name for column in database_columns]

                # Check if each field in the model exists in the database table
                for field in model._meta.fields:
                    if field.column not in database_column_names:
                        # Add the new column to the table
                        schema_editor.add_field(model, field)

db = Database(
    engine="django.db.backends.mysql",
    name=os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    host="localhost"
)

def close_db(signal,frame):
    print("closing DB",file=sys.stderr)
    close_old_connections()
    
signal.signal(signal.SIGINT,close_db)   
signal.signal(signal.SIGTERM,close_db)

class CustomBaseModel(Model):
    class Meta:
        app_label=""
        abstract = True
        
class Record(CustomBaseModel):
    id = models.BigAutoField(primary_key=True)
    externalId = models.CharField(max_length=64,db_index=True)
    recordType = models.CharField(max_length=64,db_index=True)
    shopifyId = models.CharField(max_length=255,db_index=True)
    recordAlternativeId = models.CharField(max_length=255,db_index=True)
    recordClassification= models.CharField(max_length=255)
    created = models.DateTimeField(default=datetime.datetime.now)
    updated = models.DateTimeField(default=datetime.datetime.now)
    data = models.JSONField(null=True,default="{}")
    consolidated = models.JSONField(null=True,default="{}")
    use_as_test = models.BooleanField(default=False)
    tranch = models.CharField(max_length=4,default="1970",db_index=True)
    segment = models.IntegerField(default=0,db_index=True)
    errors = models.TextField(default="")
    def getData(self):
        return SearchableDict(json.loads(self.data))
    def setData(self,data):
        if isinstance(data,dict):
            self.data = json.dumps(data)
        elif isinstance(data,SearchableDict):
            self.data = json.dumps(data.data)
    
    class Meta(CustomBaseModel.Meta):
        db_table="record"
        indexes = [
            models.Index(fields=['recordType','externalId']),
            models.Index(fields=["shopifyId","tranch"]),
            models.Index(fields=["tranch","segment"]),
            models.Index(fields=["tranch","segment","shopifyId"]),
            models.Index(fields=["recordType","recordClassification","shopifyId"]),
            models.Index(fields=["recordType","recordClassification","recordAlternativeId"]),
            models.Index(fields=["recordType","recordClassification"]),
        ]
        get_latest_by = 'externalId'
        
        
class MetafieldMapping(CustomBaseModel):
    id = models.BigAutoField(primary_key=True)
    
    sourcePath = models.CharField(max_length=255)
    namespace=models.CharField(max_length=64)
    key=models.CharField(max_length=128)
    type=models.CharField(max_length=128)
    value = models.TextField(null=False,default="")
    
    class Meta(CustomBaseModel.Meta):
        
        db_table = "metafield_mapping"
        
class FieldMapping(CustomBaseModel):
    
    id = models.BigAutoField(primary_key=True)
    
    sourcePath = models.CharField(max_length=255)
    destPath = models.CharField(max_length=255)
    
    class Meta(CustomBaseModel.Meta):
        db_table = "field_mapping"
        
class ProductInfo(CustomBaseModel):
    id = models.BigAutoField(primary_key=True)
    productId = models.CharField(max_length=255)
    variantId = models.CharField(max_length=255)
    title = models.TextField(default="")
    SKU = models.CharField(max_length=64,db_index=True)
    price = models.DecimalField(max_digits=10,decimal_places=2)
    locationId = models.CharField(max_length=255,default="")
    
    class Meta(CustomBaseModel.Meta):
        db_table = "productInfo"
        get_latest_by = 'id'

class CustomerLookup(CustomBaseModel):
    id = models.BigAutoField(primary_key=True)
    email = models.CharField(max_length=255,db_index=True)
    customerId = models.CharField(max_length=255)
    
    class Meta(CustomBaseModel.Meta):
        db_table = "emailLookup"
        
class BadOrders(CustomBaseModel):
    id = models.BigAutoField(primary_key=True)
    orderId = models.CharField(max_length=128)
    name = models.CharField(max_length=64)
    created = models.DateTimeField(default=datetime.datetime.now,blank=True)
    tags = models.TextField(default="")
    appName = models.CharField(max_length=255)
    class Meta(CustomBaseModel.Meta):
        db_table = "badOrders"
        
class CreatorInstance(CustomBaseModel):
    id = models.BigAutoField(primary_key=True)
    recordClass = models.CharField(max_length=255,db_index=True)
    tranch = models.CharField(max_length=4,default="1970",db_index=True)
    segment = models.IntegerField(default=0,db_index=True)
    pid = models.IntegerField(default=0)
    
    class Meta(CustomBaseModel.Meta):
        db_table = "creator_instance"
        indexes = [
            models.Index(fields=["recordClass","tranch","segment"]),
        ]
def createModels():
    for table in [CreatorInstance,Record,FieldMapping,MetafieldMapping,ProductInfo,CustomerLookup,BadOrders]:
        try:
            db.create_table(table)
        except Exception as e:
            traceback.print_exc()
                   
if __name__=="__main__":
    
    print("hey now")
    for table in [CreatorInstance,Record,FieldMapping,MetafieldMapping,ProductInfo,CustomerLookup,BadOrders]:
        try:
            db.create_table(table)
        except Exception as e:
            traceback.print_exc()
