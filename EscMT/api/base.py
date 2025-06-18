import traceback
import requests
import json
from ..misc import SearchableDict

class RestClient:
    def __init__(self,key):
        self.api_key = key
    def headers(self):
        return {
		    	"X-Recharge-Version": "2021-11",
			    "Content-Type": "application/json",
			    "X-Recharge-Access-Token": self.api_key
		    }
    def baseUrl(self):
        return 'https://yams.com'
    def url(self,path): 
        return f"{self.baseUrl()}/{path}"
    def get(self,path):
        return self.processResponse(requests.get(self.url(path),headers=self.headers()))
    def post(self,path,body={}):
        return self.processResponse(requests.post(self.url(path),data=json.dumps(body),headers=self.headers()))
    def put(self,path,body={}):
        return self.processResponse(requests.put(self.url(path),data=json.dumps(body),headers=self.headers()))
    def delete(self,path):
        return requests.delete(self.url(path),headers=self.headers()).status_code
    
    def getErrors(self,error:requests.Response):
        return {
            "errors":[
                {
                    "code":error.status_code,
                    "message":error.reason,
                    "body":error.content.decode("urf-8")
                }
            ]
        }
    def processResponse(self,response:requests.Response):
        try:
            res = response.json()
            if isinstance(res,list):
                if len(res)==1:
                    return SearchableDict(res[0])
                else:
                    return SearchableDict({"data":res})
            else:
                return SearchableDict(response.json())
        except requests.HTTPError as e:
            return SearchableDict(self.getErrors(response))
        except:
            return SearchableDict(self.getErrors(response))
            return None
        