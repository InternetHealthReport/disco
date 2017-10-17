
from pymongo import MongoClient,ASCENDING
from datetime import datetime
import ipaddress
import configparser
import traceback
import sys
class mongoClient():
    def __init__(self,dbname):
        self.client = MongoClient(dbname)
        self.db = self.client.disco
        self.dbatlas = self.client.atlas
        self.posts = self.db.posts

    def getTraceroutes(self,start,end,probeID,msmID):
        returnList=[]
        dayStr=datetime.utcfromtimestamp(float(start)).strftime("%Y%m%d")
        collection='tracerouteNew_'+str(dayStr)
        documents=self.db[collection].find({"prb_id":probeID,"msm_id":msmID})
        for doc in documents:
            if doc['timestamp']>=start and doc['timestamp']<=end:
                returnList.append(doc)
        return returnList

    def getTraceroutesAtlasDB(self,start,end,probeID,msmID):
        returnList=[]
        year,month,day=datetime.utcfromtimestamp(float(start)).strftime("%Y-%m-%d").split('-')
        collection='tracerouteNew_'+str(year)+'_'+str(month)+'_'+str(day)
        documents=self.dbatlas[collection].find({"prb_id":{"$in":probeID},"timestamp":{"$gte": start, "$lte": end}})
        for doc in documents:
            if doc['msm_id']==msmID:# and doc['timestamp']>=start and doc['timestamp']<=end:
                returnList.append(doc)
        return returnList

    def getPingOutages(self,start,end,prefix):
        returnList=[]
        eleven=float(30*60)
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
                #sys.stdout.flush()
                for doc in documents:
                    if doc['outageStart']>=(start-eleven) and doc['outageEnd']<=(end+eleven):
                        #print(doc)
                        returnList.append(blockPrefix)
        except:
            traceback.print_exc()
        return returnList

    def getPingOutagesByDuration(self,prefix,collection,duration):
        returnList=[]
        try:
            uprefix=unicode(prefix)
            documents=self.db[collection].find({"outagePrefix":str(uprefix),'japan':1,'california':1,'colorado':1,'greece':1})
            for doc in documents:
                if float(doc['duration'])>=duration:
                    returnList.append(doc)
        except:
            traceback.print_exc()
        return returnList

    def createIndexes(self):
        collections=set()
        try:
            documents=self.db.collection_names()
            for name in documents:
                if 'pingoutage_' in name:
                    collections.add(name)
        except:
            traceback.print_exc()
        for collc in collections:
            self.db[collc].create_index([("outagePrefix", ASCENDING)])
            print(collc)

    def createIndexesOnAtlas(self):
        collections=set()
        try:
            documents=self.dbatlas.collection_names()
            for name in documents:
                if 'traceroute_' in name:
                    collections.add(name)
        except:
            traceback.print_exc()
        for collc in collections:
            self.dbatlas[collc].create_index([("prb_id", ASCENDING)])
            self.dbatlas[collc].create_index([("msm_id", ASCENDING)])
            print(collc)
            sys.stdout.flush()

    def createIndexesOnTraceroutes(self):
        collections=set()
        try:
            documents=self.db.collection_names()
            for name in documents:
                if 'tracerouteNew_' in name:
                    collections.add(name)
        except:
            traceback.print_exc()
        for collc in collections:
            self.db[collc].create_index([("prb_id", ASCENDING)])
            self.db[collc].create_index([("msm_id", ASCENDING)])
            print(collc)
            sys.stdout.flush()

    def createIndexesOnCollection(self,collection):
        self.db[collection].create_index([("outagePrefix", ASCENDING)])
        print(collection)

    def checkPrefixWasProbed(self,prf,start):
        boolRet=True
        #1427961727
        dayStr=datetime.utcfromtimestamp(start).strftime("%Y%m%d")
        collection='pingoutageall_'+str(dayStr)
        documents=self.db[collection].find_one({"outagePrefix":prf})
        if not documents:
            boolRet=False
        return boolRet

    def insertTraceroutes(self,collection,traceroutes):
        self.db[collection].insert_many(traceroutes)

    def insertPingOutage(self,collection,outageInfo):
        self.db[collection].insert(outageInfo)

    def getCollectionsLike(self,collectionStrMatch):
        collections=self.db.collection_names()
        toReturnCollections=[]
        for collc in collections:
            if collectionStrMatch in collc:
                toReturnCollections.append(collc)

        return toReturnCollections

    def insertLiveResults(self,collection,results):
        self.db[collection].insert(results)

if __name__ == "__main__":
    configfile = 'conf/mongodb.conf'
    config = configparser.ConfigParser()
    try:
        config.sections()
        config.read(configfile)
    except:
        logging.error('Missing config: ' + configfile)
        exit(1)

    try:
        DBNAME = eval(config['MONGODB']['dbname'])
    except:
        print('Error in reading conf. Check parameters.')
        exit(1)

    mongodb=mongoClient(DBNAME)
    #mongodb.createIndexesOnCollection('pingoutageall_20150402')
    mongodb.createIndexesOnTraceroutes()