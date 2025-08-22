import urllib.error
import shopify
import json
from ..misc import *
import logging
import urllib
import time
import signal
import http.client
logger = logging.getLogger(__name__)

def sigInt(signal,frame):
    sys.exit()

signal.signal(signal.SIGINT,sigInt)

def log(message):
    print(message,flush=True)

def catchNetWorkError(fn):
    def wrapper(self, *args, **kwargs):
        retry = True
        retryCount = 0
        while retry and retryCount<10:
            try:
                ret:GqlReturn = fn(self,*args, **kwargs)
                if ret.isDevThrottled():
                    retry = True
                    retryCount = retryCount + 1
                    log(f"dev throttle: retrying {retryCount}/10")
                    time.sleep(20)
                else:
                    retry = False
                    return ret
            except urllib.error.HTTPError as e:
                retryCount = retryCount + 1
                log(f"retrying {retryCount}/10")
                time.sleep(3)
            except urllib.error.URLError as e:
                retryCount = retryCount + 1
                log(f"retrying {retryCount}/10")
                time.sleep(3)
            except http.client.RemoteDisconnected:
                retryCount = retryCount + 1
                log(f"retrying {retryCount}/10")
                time.sleep(3)
            except KeyboardInterrupt:
                sys.exit()
            except Exception as e:
                retryCount = retryCount + 1
                log(f"retrying {retryCount}/10")
                time.sleep(3)
    wrapper.__name__ = fn.__name__
    return wrapper
    
class GraphQL:
    def __init__(self,debug=False,searchable=True,minThrottle=1000):
        self.debugging = debug
        self.debuggingIndent=1
        self.searchable = searchable
        self.minThrottleAvailable=minThrottle
    def debug(self,value=True,level=1):
        self.debugging = value
        self.debuggingIndent = level
        
    @catchNetWorkError    
    def run(self,query,variables={},searchable=True,throttle=5000):
        ret = json.loads(shopify.GraphQL().execute(query,variables))
        retVal = GqlReturn(ret)
        

        if throttle is not None:
            if retVal.throttleRemaining()<throttle:
                log(f"throttling at {retVal.throttleRemaining()}")
                time.sleep(2)
        
        return retVal
    def iterable(self,query,params,dataroot="data.products"):
        return GraphQlIterable(query,params,dataroot=dataroot)
        pass
    
        
class GraphQlIterable(GraphQL):
    def __init__(self, query,params,dataroot="data.products"):
        super().__init__(searchable=True,debug=False)
        self.cursor = None
        self.hasNext = True
        self.query = query
        self.params = params
        self.dataroot = dataroot
    def __iter__(self):
        return self
    def __next__(self):
        if not self.hasNext:
            raise StopIteration
        self.params["after"] = self.cursor
        ret = self.run(self.query,self.params)
        
        
        if ret.get("data") is not None:
            try:
                self.dataroot = f'data.{next(iter(ret.search("data").keys()),None)}'
            except:
                ret.dump()
                sys.exit()
        
        if ret.hasErrors():
            print(json.dumps(ret.errorMessages(),indent=1))
        values = [GqlReturn(x) for x in ret.search(f"{self.dataroot}.nodes",[])]
        
        if ret.search(f"{self.dataroot}.pageInfo.hasNextPage"):
            self.hasNext = True
            self.cursor = ret.search(f"{self.dataroot}.pageInfo.endCursor")
            
        else:
            self.hasNext = False
        return values
        
        
