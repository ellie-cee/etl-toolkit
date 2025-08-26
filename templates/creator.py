#!/usr/bin/env python

from argparse import ArgumentParser
from EscMT.shopify.operations import *
from EscMT.models import *


from argparse import ArgumentParser
from EscMT.shopify.operations import *
from EscMT.models import *
from EscMT.shopify.project import *

parser = ArgumentParser()
parser.add_argument("--recordType",required=True)
parser.add_argument("--profile",default="default")
parser.add_argument("--sourceClass",default="source")
parser.add_argument("--recordId")
parser.add_argument("--limit",default=None)
args = parser.parse_args()

processor = ProjectCreatorOptions()

match args.recordType:
    case "customer":
        ShopifyCustomerCreator(
            profile=args.profile,
            sourceClass=args.sourceClass,
            processor=processor,
            limit=args.limit
        ).run()
        
    case "product":
        ShopifyProductCreator(
            profile=args.profile,
            sourceClass=args.sourceClass,
            processor=processor,
            limit=args.limit
        ).run()
    case "order":
        creator = ShopifyOrderCreator(
            profile=args.profile,
            sourceClass=args.sourceClass,
            processor=processor,
            limit=args.limit
        )
if args.recordId is not None:
    record = Record.objects.get(numericId=args.recordId)
    if record is not None:
        creator.processRecord(record)
    else:
        print("what now")
        creator.run()
            
        