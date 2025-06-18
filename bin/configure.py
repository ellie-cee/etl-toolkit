#!/usr/bin/env python3

import os
import sys
import json
from urllib.parse import urlparse
from password_generator import PasswordGenerator
import subprocess
import heroku3
import toml
import json
from slugify import slugify
mysql = subprocess.check_output(["heroku","config:get","JAWSDB_URL"]).decode("utf-8").strip()
if len(mysql)<1:
    mysql = subprocess.check_output(["heroku","config:get","JAWSDB_MARIA_URL"]).decode("utf-8").strip()
if len(mysql)<1:
    print("No Mysql Configuration found")
    sys.exit(0)
    
app_domain = subprocess.check_output(["heroku","domains"]).decode("utf-8").strip().split("\n")[-1]

url = urlparse(mysql)
config = {}
for line in open(".env-default").readlines():
    if "=" not in line:
        continue
    line = line.replace("\n","").strip()
    key = line[0:line.index("=")]
    value = line[line.index("=")+1:len(line)].strip()
    config[key] = value
    



config["DB_NAME"] = url.path[1:len(url.path)]
config["DB_HOST"] = url.hostname
config["DB_USER"] = url.username
config["DB_PASSWORD"] = url.password
config["DJANGO_SECRET"] = PasswordGenerator().generate()
config["APP_URL"] = f"https://{app_domain}/"
config["APP_HOST"] = app_domain
config["JAWSDB_MARIA_URL"] = mysql
config["HEROKU_APP_NAME"] = "-".join(app_domain.split(".")[0].split("-")[0:-1])
config["APP_NAME"] = input("Shopify App Name: ").replace("\n","").strip()
config["SHOPIFY_API_KEY"] = input("Shopify App Client ID: ").replace("\n","").strip()
config["SHOPIFY_API_SECRET"] = input("Shopify App Secret: ").replace("\n","").strip()
store_name = input("Shopify Store Hostname (<store-name>.myshopify.com): ").replace("\n","").strip()
config["SHOPIFY_DOMAIN"] = f'{store_name}{".myshopify.com" if not "myshopify" in store_name else ""}'
config["SHOPIFY_CORS_DOMAIN"] = f'Live site domain"}'
config["HEROKU_KEY"] = input("Heroku API key: ").replace("\n","").strip()

print("Writing .env")
output = open(".env","w")
for key in config.keys():
    print(f"{key}={config[key]}",file=output)
output.close()

print("Configuring Heroku...")
heroku = heroku3.from_key(os.environ.get("HEROKU_KEY"))

ret = heroku.update_appconfig(
    config.get("HEROKU_APP_NAME"),
    {key:config[key] for key in filter(lambda x: x not in ["HEROKU_KEY","HEROKU_APP_NAME","SHOPIFY_TOKEN"],config.keys()) }
)

print("Writing shopify.app.toml")

shopify_app = toml.load("shopify-extensions/shopify.app.toml")
shopify_app["client_id"] = config["SHOPIFY_API_KEY"]
shopify_app["name"] = config["APP_NAME"]
shopify_app["handle"] = slugify(config["APP_NAME"])
shopify_app["application_url"] = config["APP_URL"]
shopify_app["auth"]["redirect_urls"] = [f"{config['APP_URL']}{x}" for x in ["shopify/auth/","shopify/login/","shopify/finalize/","shopify/logout/"]]

print(toml.dumps(shopify_app))

open("shopify-extensions/shopify.app.toml","w").write(toml.dumps(shopify_app))

print("Applying Migrations...")
ret = subprocess.check_output(["./manage.py","migrate"]).decode("utf-8").strip()
print(ret)

   
   
    
