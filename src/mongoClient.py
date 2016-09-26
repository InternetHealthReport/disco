from pymongo import MongoClient
from datetime import datetime
import ipaddress
import traceback
import sys
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
            if doc['timestamp']>=start and doc['timestamp']<=end:
                returnList.append(doc)
        return returnList

    def getPingOutages(self,start,end,prefix):
        returnList=[]
        eleven=float(11*60)
        dayStr=datetime.utcfromtimestamp(float(start)).strftime("%Y%m%d")
        collection='pingoutage_'+str(dayStr)
        try:
            #print(prefix,type(prefix))
            uprefix=unicode(prefix)
            network = ipaddress.IPv4Network(uprefix)
            all24 = [network] if network.prefixlen >= 24 else network.subnets(new_prefix=24)
            for blockPrefix in all24:
                #print(collection)
                #sys.stdout.flush()
                documents=self.db[collection].find({"outagePrefix":str(blockPrefix)})
                #print(type(documents))
                sys.stdout.flush()
                for doc in documents:
                    if doc['outageStart']>=(start-eleven) and doc['outageEnd']<=(end+eleven):
                        #print(doc)
                        returnList.append(blockPrefix)
        except:
            traceback.print_exc()
        return returnList

    def checkPrefixWasProbed(self,prf):
        boolRet=True
        dayStr=datetime.utcfromtimestamp(1427961727).strftime("%Y%m%d")
        collection='pingoutageall_'+str(dayStr)
        documents=self.db[collection].find_one({"outagePreifx":prf})
        if not documents:
            boolRet=False
        return boolRet

    def insertTraceroutes(self,collection,traceroutes):
        self.db[collection].insert_many(traceroutes)

    def insertPingOutage(self,collection,outageInfo):
        self.db[collection].insert(outageInfo)

if __name__ == "__main__":
    mongodb=mongoClient()