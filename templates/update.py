#!/usr/bin/env python

from argparse import ArgumentParser
from EscMT.shopify.operations import *
from EscMT.shopify.project import *
from EscMT.models import *
from EscMT.graphQL import *
from django.db.models import Q

parser = argparse.ArgumentParser()
parser.add_argument("--sourceProfile",default="default")
parser.add_argument("--destProfile",required=True)
args = parser.parse_args()

updater = ShopifyProductSync()
updater.run(sourceProfile=args.sourceProfile,destProfile=args.destProfile)


