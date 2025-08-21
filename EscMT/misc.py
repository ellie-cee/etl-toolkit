import json
import os
import pathlib
import sys
from jmespath import search as jpath
import shopify
import phonenumbers
import pycountry
from unidecode import unidecode
from dict_recursive_update import recursive_update
from typing import List
import datetime
from decimal import *
import mysql.connector


can_province_names = {"Alberta": "AB", "British Columbia": "BC", "Manitoba": "MB", "New Brunswick": "NB", "Newfoundland and Labrador": "NL", "Northwest Territories": "NT", "Nova Scotia": "NS", "Nunavut": "NU", "Ontario": "ON", "Prince Edward Island": "PE", "Quebec": "QC", "Saskatchewan": "SK", "Yukon": "YT"}
can_province_codes = dict(map(reversed, can_province_names.items()))
us_state_names = {"Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC", "American Samoa": "AS", "Guam": "GU", "Northern Mariana Islands": "MP", "Puerto Rico": "PR", "United States Minor Outlying Islands": "UM", "U.S. Virgin Islands": "VI"}
us_state_codes = dict(map(reversed, us_state_names.items()))


class bp:
    def __init__(self):
        self.bpc = 0
    def inc(self):
        self.bpc = self.bpc+1
        
        

        
class SearchableDict:
    def __init__(self,data):
        if data is None:
            return None
        for k in data.keys():
            if not hasattr(self,k):
                setattr(self,k,data[k])
        self.data = data
    def search(self,path,default=None):
        ret =  jpath(path,self.data)
        if ret is None:
            return default
        return ret
    def has(self,key):
        return hasattr(self,key)
    def get(self,key,default=None):
        if hasattr(self,key) and getattr(self,key) is not None:
            return getattr(self,key)
        else:
            return default
    def valueOf(self,key):
        ret = self.get(key)
        if ret is dict and self.search(f"{key}.refName"):
            return self.search(f"{key}.refName")
        else:
            return ret
    def dump(self,printIt=True):
        if printIt:
            print(json.dumps(self.data,indent=1),file=sys.stderr)
        else:
            return self.data
    def set(self,key,value):
        paths = list(reversed(key.split(".")))
        if len(paths)>1:
            object = value
            for k in paths:
                object = {k:object}
            self.data = recursive_update(self.data,object)
        else:
            self.data[key] = value
    def getAsSearchable(self,key,default={}):
        val = self.search(key)
        if isinstance(val,list):
            return [SearchableDict(x) for x in val]
        if isinstance(val,dict):
            return SearchableDict(val)
        if val is None:
            return None
        return val
    
    def append(self,key,value):
        myValue = self.search(key,[])
        myValue.append(value)
        self.set(key,myValue)
        return
        if key not in self.data:
            self.data[key] = value
        elif type(self.data[key]) is not list:
            self.data[key] = [self.data[key],value]
        else:
            self.data[key].append(value)
    @staticmethod
    def fromList(list):
        return [SearchableDict(x) for x in list]
    
class GqlReturn(SearchableDict):
    def errors(self,dump=False):
        if not hasattr(self,"errorDetails"):
            errorDetails = self.findErrors(self.data)
            setattr(self,"errorDetails",errorDetails)
            if dump:
                print(json.dumps(errorDetails,indent=1),file=sys.stderr)
               
        return self.errorDetails
        
    def findErrors(self,object):
        if isinstance(object, dict):
            if "userErrors" in object:
                return object["userErrors"]
            elif "errors" in object:
                return [{"message":x.get("message"),"code":"NA","field":SearchableDict(x).search("problems[0].path[-1]")} for x in object.get("errors")]
            for key in object:
                item = self.findErrors(object[key])
                if item is not None:
                    return item
            
        elif isinstance(object, list):  
            for element in object:
                item = self.findErrors(element)
                if item is not None:
                    return item
        return None
    def errorMessages(self):
        if self.errors() is None:
            return []
        return [x.get("message") for x in self.errors()]
    def errorCodes(self):
        if self.errors() is None:
            return []
        return [x.get("code") for x in self.errors()]
    def hasErrorCode(self,code):
        return code in self.errorCodes()
    def hasErrors(self):
        return self.errors() is not None and  len(self.errors())>0
    def nodes(self,path):
        return [GqlReturn(x) for x in self.search(f"{path}.nodes",[])]
    def throttleRemaining(self):
        return self.search("extensions.cost.throttleStatus.currentlyAvailable",0)
    
    def isDevThrottled(self):
        errors = self.findErrors(self.data)
        if errors is not None:
            for error in errors:
                if "Too many attempts" in error.get("message"):
                    return True
        return False
        
        
        
def partition(allrows,chunksize=4):
        ret = []
        
        total = len(allrows)
        
        if (total<chunksize):
            return [allrows]
            
        chunks = int(len(allrows)/chunksize)+ 1 if total%chunksize>0 else int(len(allrows)/chunksize)
        
        for i in range(chunks):
            slicer = slice
            ret.append(allrows[:chunksize])
            
            allrows = allrows[chunksize:]
        return ret
def is_phone(phone):
    return phonenumbers.is_possible_number_string(phone,'US')

def format_phone(phone):
    try:
        return phonenumbers.format_number(phonenumbers.parse(phone,'US'),phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.phonenumberutil.NumberParseException:
        print(f"well this number just wrecked everything: {phone}",file=sys.stderr)
        return ""
def country_code(country):
    ret = pycountry.countries.get(name=country)
    if ret is not None:
        return ret.alpha_2
def logJSON(data):
    print(json.dumps(data,indent=1),file=sys.stderr)
    
def fixAddress(address):
    if len(address["countryCode"])>3:
        address["countryCode"] = country_code(address["countryCode"])
    if not address.get("zoneCode"):
        return address    
    if len(address["zoneCode"])>3:
        possibilities = list(filter(lambda x:unidecode(x.name)==address["zoneCode"] or address["zoneCode"] in unidecode(x.name),pycountry.subdivisions.get(country_code=address["countryCode"])))
        if len(possibilities)>0:
            address["zoneCode"] = possibilities[0].code.split("-")[-1]
            if address["zoneCode"] == "SJ":
                address["zoneCode"] = None
    
     
    if "phone" in address:
        if address["phone"].startswith("555") or address["phone"].startswith("+555"):
            del address["phone"]
        elif is_phone(address["phone"]) and not address["phone"] is None:
            if is_phone(address["phone"]):
                address["phone"] = format_phone(address["phone"])
        else:
            del address["phone"]
    return address

def stripShopify(record,stripExternal=False,translate={},extra=[]):
        stripFields = ["shopifyId","shopifyCustomerId","recipient","id"]
        
        if stripExternal:
            stripFields.append("externalId")
        if record.get("externalId") is None:
            if "externalId" in record:
                del record["externalId"]
        ret =  {key:record[key] for key in filter(lambda x:not x in stripFields and not x.startswith("_") and x not in extra,record.keys())}
        for key,value in translate.items():
            if key in ret:
                
                ret[value] = record[key]
                del ret[key]
        if "shippingAddress" in ret:
            ret["shippingAddress"] = fixAddress(stripShopify(ret["shippingAddress"],stripExternal=True,translate=translate))
        if "billingAddress" in ret:
            ret["billingAddress"] = fixAddress(stripShopify(ret["billingAddress"],stripExternal=True,translate=translate))
        if ret.get("billingSameAsShipping") and ret.get("billingAddress"):
            del ret["billingAddress"]
        if "externalId" in ret:
            ret["externalId"] = str(ret.get("externalId"))
        phone = ret.get("phone")
        if phone is not None:
            if phone=="":
                del ret["phone"]
            elif phone.startswith("555") or phone.startswith("1555") or phone.startswith("+555"):
                del ret["phone"]
            elif not phone.startswith("+"):
                if is_phone(ret["phone"]):
                    ret["phone"] = format_phone(phone)
                else:
                    del ret["phone"]
            
        return ret
def loadProfiles():
    if pathlib.Path(".shopify-profiles.json").exists():
        return SearchableDict(json.load(open(".shopify-profiles.json")))
    else:
        return None
    
def shopifyInit(useProfile="default"):
    
    connectionDetails = None
    profiles = loadProfiles()
    
    if profiles is not None:
        profile = useProfile
        if useProfile is not None:
            profile = useProfile
        else:
            if os.environ.get("PROFILE") is not None:
                profile = os.environ.get("PROFILE")        

        if profiles.get(profile) is not None:
            connectionDetails = profiles.get(profile)
    else:
        connectionDetails = {}
        for key,value in os.environ.items():
            if key.startswith("SHOPIFY"):
                connectionDetails[key] = value
    if connectionDetails is not None and len(list(connectionDetails.keys()))>0:
        shopify.ShopifyResource.activate_session(
            shopify.Session(
                f'{connectionDetails.get("SHOPIFY_DOMAIN")}/admin',
                connectionDetails.get('SHOPIFY_API_VERSION'),
                connectionDetails.get('SHOPIFY_TOKEN')
            )
        )

def jsonify(value):
        if isinstance(value,dict):
            ret = {}
            for key,value in value.items():
                if isinstance(value,Decimal):
                    ret[key] = float(value)
                elif isinstance(value,datetime.datetime):
                    ret[key] = str(value)
                else:
                    ret[key] = jsonify(value)
            return ret
        elif isinstance(value,list):
            return [jsonify(x) for x in value]
        else:
            return value

def database():
    db = mysql.connector.connect(
        host=os.environ.get("DB_HOST"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("DB_NAME")
    )
    return db

def stripDict(dictObject,ignore=[]):
    ret = {}
    for key,value in dictObject.items():
        if key not in ignore:
            ret[key] = value
    return ret
class JSONException(Exception):
    pass
