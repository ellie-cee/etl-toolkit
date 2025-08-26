#!/usr/bin/env python

from argparse import ArgumentParser
from EscMT.shopify.operations import *
from EscMT.shopify.project import *
from EscMT.models import *
import signal

signal.signal(signal.SIGINT,lambda x,y:sys.exit())

parser = ArgumentParser()
parser.add_argument("--recordType",required=True)
parser.add_argument("--profile",default="default")
parser.add_argument("--sourceClass",default="source")
parser.add_argument("--restart",action="store_true",default=False)
parser.add_argument("--recordId")

args = parser.parse_args()
importer = None

if args.restart:
    Record.objects.filter(recordType=args.recordType).delete()
    
processor = ProjectCreatorOptions()
recordId = args.recordId
match args.recordType:
    case "product":
        importer = ShopifyProductImporter(
            profile=args.profile,
            sourceClass=args.sourceClass,
            processor=processor
        )
    case "customer":
        importer = ShopifyCustomerImporter(
            profile=args.profile,
            sourceClass=args.sourceClass,
            processor=processor
        )
    case "order":
        importer = ShopifyOrderImporter(
            profile=args.profile,
            sourceClass=args.sourceClass,
            processor=processor
        )
    case "location":
        importer = ShopifyLocationImporter(
            profile=args.profile,
            sourceClass=args.sourceClass,
            processor=processor
        )
    
    
    

if importer is not None:
    if recordId is not None:
        importer.singleRecord(recordId)
    else:
        importer.run()