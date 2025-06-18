#!/usr/bin/env python3

import os
import sys
import json
from urllib.parse import urlparse
from password_generator import PasswordGenerator
import subprocess
import toml
import json
from slugify import slugify
import pathlib
from argparse import ArgumentParser
import mysql.connector


class Configurator:
    def __init__(self):
        pass
        
    def configure(self):
        
        config = {}        
        if pathlib.Path(".env").exists():
            existing = open(".env")
            for line in existing.readlines():
                (key,value) = line.strip().split("=")
                config[key] = value
        else:
            config["PROJECT_NAME"] = input("Project Prefix (CompanyName): ").replace("\n","").strip()
            config["DB_NAME"] = input("Database Name: ").replace("\n","").strip()
            config["DB_USER"] = input("Database User: ").replace("\n","").strip()
            config["DB_PASSWORD"] = input("Database Password: ").replace("\n","").strip()
            config["DB_HOST"] = "localhost"
            config["SHOPIFY_API_KEY"] = input("Shopify App Client ID: ").replace("\n","").strip()
            config["SHOPIFY_API_SECRET"] = input("Shopify App Secret: ").replace("\n","").strip()
            config["SHOPIFY_API_VERSION"] = input("Shopify API Version: ").replace("\n","").strip()
            config["PLATFORM"] = input("Source Platform: ").replace("\n","").strip()
            
            if config["SHOPIFY_API_VERSION"]=="":
                config["SHOPIFY_API_VERSION"] = "2025-01"
                
            config["SHOPIFY_TOKEN"] = input("Shopify Access Token: ").replace("\n","").strip()
            store_name = input("Shopify Store Hostname (<store-name>.myshopify.com): ").replace("\n","").strip()
            config["SHOPIFY_DOMAIN"] = f'{store_name}{".myshopify.com" if not "myshopify" in store_name else ""}'
    
            print("Writing .env")
            output = open(".env","w")
            for key in config.keys():
                print(f"{key}={config[key]}",file=output)
            output.close()
        print("Scaffolding directories")
        
        for newDir in [
            "bin",
            "tmp",
            "matrixify",
            "logs"
        ]:
            path = pathlib.Path(newDir)
            path.mkdir(parents=True,exist_ok=True)
            path.chmod(0o777)
            
            
        print("Generating Iterators")
        
        self.generateIterator("Product",config)
        self.generateIterator("Customer",config)
        self.generateIterator("Company",config)
        self.generateIterator("Product",config)
        self.generateIterator("Product",config)
        
        print("Generating Consolidators")
        
        self.generateProcesssors(config,"Consolidate","Product",["ProductRecordAwareClient"])
        self.generateProcesssors(config,"Consolidate","Customer",["CustomerRecordAwareClient","AddressAwareClient"])
        self.generateProcesssors(config,"Consolidate","Order",["OrderRecordAwareClient","AddressAwareClient"])
        
        print("Generating Creators")
        
        self.generateProcesssors(config,"Create","Product",["ProductRecordAwareClient"])
        self.generateProcesssors(config,"Create","Customer",["CustomerRecordAwareClient","AddressAwareClient"])
        self.generateProcesssors(config,"Create","Company",["CustomerRecordAwareClient","AddressAwareClient"])
        self.generateProcesssors(config,"Create","Order",["OrderRecordAwareClient","AddressAwareClient"])
        self.generateProcesssors(config,"Create","Navigation",["BaseRecord"])
        self.generateProcesssors(config,"Create","Collection",["BaseRecord"])
        
        print("initializing database")
        try:
            db = mysql.connector.connect(
	            host=os.environ.get("DB_HOST"),
	            user=os.environ.get("DB_USER"),
	            password=os.environ.get("DB_PASS"),
	            database=os.environ.get("DB_NAME")
            )
            db = mysql.connector.connect(
	            host="localhost",
	            user="uch_user",
	            password="gunsgunsguns",
	            database="uch_dataurrrr"
            )
        except:
            print("Creating database")
            #self.createDatabase(config)
        
        
        
        
        
        
    def generateIterator(self,type,config):
        className = f"{type}Iterator"
        contents = f"""
{self.preamble()}
class {type}Iterator(RecordIterator):
    def __init__():
        super().__init__(self,**args)
        
    
iterator = {type}Iterator()
iterator.run()
        """
        filePath = f"bin/iterate{type}.py"
        open(
            filePath,
            "w"
        ).write(contents)
        pathlib.Path(filePath).chmod(0o777)
        
    def generateProcesssors(self,config,processorType,type,baseClasses=[]):
        className = f"{config.get("PROJECT_NAME")}{type}{processorType}"
        content = f"""
{self.preamble()}

parser = argparse.ArgumentParser()
parser.add_argument("--only","-o",action="append",default=[])
parser.add_argument("--selector","-s",default=None)
parser.add_argument("--draft",action="store_true")
parser.add_argument("--revision",default="none")
args = vars(parser.parse_args())


class {className}({",".join(baseClasses)}):
    def __init__(self,**args):
        super().__init__(self,**args)
    
        
operator = {className}(**args)
operator.run()
        
        """
        filePath = f"bin/{type.lower()}{processorType}.py"  
        open(
            filePath,
            "w"
        ).write(content)
        pathlib.Path(filePath).chmod(0o777)
        
        
    def createDatabase(self,config):
        open(
            "tmp/create.sql",
            "w"
        ).write(
            f""" 
                DROP DATABASE IF EXISTS `{config.get('DB_NAME')}`;
                CREATE DATABASE `{config.get('DB_NAME')}`; 
                DROP USER IF EXISTS `{config.get('DB_USER')}`@`localhost`;
                create user `{config.get('DB_USER')}`@`localhost` IDENTIFIED BY '{config.get('DB_PASSWORD')}';
                GRANT ALL PRIVILEGES on {config.get('DB_NAME')}.* to `{config.get('DB_USER')}`@`localhost`;
                flush privileges;
            """)
        os.system("sudo mysql < tmp/create.sql")
        
    def preamble(self):
        
        return 
"""
import traceback
from pathlib import Path
if __package__ is None:                  
    DIR = Path(__file__).resolve().parent
    sys.path.insert(0, str(DIR.parent))
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'shopify_django_app.settings')
    
    try:
        from EscMT import *
        
    except ImportError as exc:
        raise ImportError(
            "Couldn't import EscMT"
        ) from exc

    __package__ = DIR.name

   
import time
import urllib.error
import urllib.error
from unidecode import unidecode
from urllib.parse import quote as urlQuote
import datetime
from dateutil.parser import parse as parseDate
import pandas
import urllib
import xmltodict
import functools
import math
import os
import sys
import traceback
import requests
from glob import glob as listFiles
from jmespath import search as jpath
from bs4 import BeautifulSoup
from slugify import slugify
import argparse
import json
"""


if __name__=="__main__":
    
    
    Configurator().configure()
   
   
    
