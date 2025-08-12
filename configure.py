#!/usr/bin/env python3

import os
import sys
import json
from urllib.parse import urlparse
from password_generator import PasswordGenerator
import subprocess
import toml
import json
import pathlib
from slugify import slugify
from EscMT.models import *
from django.conf import settings

    
def getValue(key,default=None,label=None,mustBe=[]):
    value = None
    while value is None:
        il = f"{label if label is not None else key} "
        if default is not None and default!="":
            il = f"{il} ({default})"
        inputValue = input(f"{il}: ").replace("\n","").strip()
        if inputValue == "":
            if default is not None and default!="":
                value = default
        elif len(mustBe)>0 and inputValue in mustBe:
            value = inputValue
        else:
            value = inputValue
    return value

def buildShopifyProfile(profile={}):
    info = {
        "SHOPIFY_API_KEY":"API Key",
        "SHOPIFY_API_SECRET":"API Secret",
        "SHOPIFY_TOKEN":"Access token",
        "SHOPIFY_DOMAIN":"Domain",
        "SHOPIFY_API_VERSION":"API Version"
    }
    for key,value in info.items():
        profile[key] = getValue(key,default=profile.get(key),label=value)
    return profile

config = {}
profiles = {"default":{}}
if pathlib.Path(".shopify-profiles.json").exists():
    profiles = json.load(open(".shopify-profiles.json"))
    
print("Default Profile")

profiles["default"] = buildShopifyProfile(profiles["default"])
for key,profile in profiles.items():
    if key == "default":
        continue
    if getValue("",default="N",label=f"Update profile '{key}'?")=="Y":
        profiles[key] = buildShopifyProfile(profiles.get(key))
    
continueAdd = True
while continueAdd:
    if getValue("",default="N",label="Add new shopify profile?")=="N":
        continueAdd = False
    else:
        profiles[getValue("",label="Profile Name")] = buildShopifyProfile({})
        
json.dump(
    profiles,
    open(".shopify-profiles.json","w"),
    indent=1
)
readFilename = ".env"
if not pathlib.Path(readFilename).exists():
    readFilename = ".env-basic"
    
for line in open(readFilename).readlines():
    if "=" not in line:
        continue
    if "SHOPIFY" in line:
        continue
    line = line.replace("\n","").strip()
    key = line[0:line.index("=")]
    value = line[line.index("=")+1:len(line)].strip()
    config[key] = getValue(key,default=value)
    
if config.get("profile") is None:
    config["PROFILE"] = "default"

for key,value in profiles.get("default").items():
    config[key] = value

print("Writing .env")
output = open(".env","w")
for key in config.keys():
    os.environ[key] = config[key]
    print(f"{key}={config[key]}",file=output)
output.close()

sqlTmpFile = "/tmp/create-esc-etl.sql"
open(
    sqlTmpFile,
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
os.system(f"sudo mysql < {sqlTmpFile}")
pathlib.Path(sqlTmpFile).unlink()
createModels()



   
   
    
