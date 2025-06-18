"""import json
import subprocess
from ..base.client import *

from ..graphQL import Customer,Companies,MetaField
from ..misc import *
from shopify_uploader import ShopifyUploader
import urllib.parse

class CollectionsCreator(RecordAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()
    """