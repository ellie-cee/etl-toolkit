#!/usr/bin/env python

from argparse import ArgumentParser
from EscMT.shopify.operations import *
from EscMT.models import *
from EscMT.graphQL import GraphQL
parser = ArgumentParser()
parser.add_argument("--recordType",required=True)
parser.add_argument("--profile",default="default")
parser.add_argument("--id",required=False)

args = parser.parse_args()
importer = None

if args.profile == "source":
    
    print("yeah, we're not deleting the source")
    sys.exit()

shopifyInit(useProfile=args.profile)

deleter = None

match args.recordType:
    case "product":
        importer = ShopifyProductImporter(profile=args.profile,sourceClass=args.sourceClass)
    case "customer":
        importer = ShopifyCustomerImporter(profile=args.profile,sourceClass=args.sourceClass)
    case "order":
        deleter = ShopifyOrderDeleter()
    case "order2":
        deleter = ShopifyOrderDeleter()
        shopifyInit(useProfile=args.profile)
        for orderGroup in GraphQL().iterable(
            """
            query getOrders($query:String,$after:String) {
                orders(first: 10,query:$query,after:$after) {
                    nodes {
                        id
                        tags
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {
                "query":"tag:'PET order'",
                "after":None
            }
        ):
            for orderRecord in orderGroup:
                deleter.delete(orderRecord.get("id"))
        sys.exit()
    case "customer2":
        deleter = ShopifyCustomerDeleter()
        shopifyInit(useProfile=args.profile)
        for orderGroup in GraphQL().iterable(
            """
            query Customers($query:String,$after:String) {
                customers(first: 250,query:$query,after:$after,reverse:true) {
                    nodes {
                        id
                        tags
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {
                "query":"tag:'PET customer'",
                "after":None
            }
        ):
            for orderRecord in orderGroup:
                deleter.delete(orderRecord.get("id"))
        sys.exit()
        
if deleter is not None:
    if args.id is not None:
        record = Record.objects.get(numericShopifyId=args.id)
        deleter.delete(order=record)
    else:
        deleter.run(all=True)