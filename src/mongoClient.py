from pymongo import MongoClient
from datetime import datetime
import traceback

class mongoClient():
    def __init__(self):
        self.client = MongoClient('mongodb-iijlab')
        self.db = self.client.disco
        self.posts = self.db.posts

    def getTraceroutes(self,start,end,probeID,msmID):
        returnList=[]
        dayStr=datetime.utcfromtimestamp(float(start)).strftime("%Y%m%d")
        collection='traceroute_'+str(dayStr)
        documents=self.db[collection].find({"prb_id":probeID,"msm_id":msmID})
        for doc in documents:
            if doc['timestamp']>start and doc['timestamp']<end:
                returnList.append(doc)
        return returnList

    def insertTraceroutes(self,collection,traceroutes):
        self.db[collection].insert_many(traceroutes)

    def insertPingOutage(self,collection,outageInfo):
        self.db[collection].insert_many(outageInfo)

if __name__ == "__main__":
    mongodb=mongoClient()