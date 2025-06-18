from .base import RestClient
import os
import json

class ShipBob(RestClient):
    def __init__(self,key,channelId=None):
        super().__init__(key)
        self.channelId=channelId
        self.remainingRequests = 150
        self.retryAfter = 0
    
    def headers(self):
        return {
            "Content-Type":"application/json",
            "Authorization":f"Bearer {self.api_key}",
            "shipbob_channel_id":self.channelId
        }
    def baseUrl(self):
        return "https://api.shipbob.com/1.0"
    def setChannel(self,channelId):
        self.channelId = channelId
    def setChannelByName(self,channel):
        channels = self.get("channel")
    def forChannel(self,channelId):
        return ShipBob(self.api_key,channelId=channelId)
    def processResponse(self, response):
        ret = super().processResponse(response)
        self.remainingRequests
        remaining = response.headers.get("X-Remaining-Calls")
        retryAfter = response.headers.get("X-Retry-After")
        if remaining is not None:
            self.remainingRequests = int(remaining)
        if retryAfter:
            self.retryAfter = int(retryAfter)
            
        return ret
            