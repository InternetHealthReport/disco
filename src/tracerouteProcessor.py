from ripe.atlas.cousteau import AtlasResultsRequest
from mongoClient import mongoClient
from datetime import datetime,timedelta
from pytz import timezone
import traceback
from contextlib import closing
from multiprocessing import Pool,cpu_count
import os
import json
from os import listdir
from os.path import isfile, join
import sys
import logging
import pprint
import networkx as nx
import ipaddress

class tracerouteProcessor():

    def __init__(self):
        #self.burstEventInfo=burstEventInfo
        self.mongodb=mongoClient()
        self.probeIDs=[]
        self.msmIDs=[]
        self.loadMsms()

    def onTracerouteResponse(self,*args):
        item=args[0]
        print(item)
        self.mongodb.insertTraceroutes(self.collection,[item])

    def getDayToProbesMapFromAggResults(self,aggResultsDir):
        dayToProbesDict={}
        if os.path.isdir(aggResultsDir):
            resultFiles = [join(aggResultsDir, f) for f in listdir(aggResultsDir) if isfile(join(aggResultsDir, f))]
            #print(resultFiles)
            for fname in resultFiles:
                with closing(open(fname,'r')) as fp:
                    for lR in fp:
                        eventLine=lR.split('|')
                        #print(eventLine)
                        probesInBurst=eval(eventLine[3])
                        dayStr=datetime.utcfromtimestamp(float(eventLine[1])).strftime("%Y-%m-%d")
                        if dayStr not in dayToProbesDict.keys():
                            dayToProbesDict[dayStr]=probesInBurst
                        else:
                            for pid in probesInBurst:
                                dayToProbesDict[dayStr].add(pid)

        return dayToProbesDict

    def loadMsms(self):
        files=['data/anchorMsmIdsv4.txt','data/builtinMsmIdsv4.txt']
        #files=['data/builtinMsmIdsv4.txt']
        for file in files:
            with open(file,'r') as fp:
                for line in fp:
                    l=int(line.rstrip('\n').split(':')[1])
                    self.msmIDs.append(l)

    def getTraceroutesFromDB(self,probeList,start,end):
        returnDict={}
        for msmID in self.msmIDs:
            for pp in probeList:
                listOfTraces=self.mongodb.getTraceroutes(start,end,pp,msmID)
                if msmID not in returnDict.keys():
                    returnDict[msmID]={}
                returnDict[msmID][pp]=listOfTraces
        return returnDict

    def toGraph(self,G,jsonTraceList):
        dst=None
        for Jdata in jsonTraceList:
            uniqIPs=[]
            try:
                uniqIPs.append(Jdata["src_addr"])
                if Jdata["dst_addr"] is not None:
                    dst=Jdata["dst_addr"].replace('.','-')

                hops=Jdata['result']
                prevIPs=set()
                pFlag=False
                initialLoadPrev=True
                for hop in hops:
                    print(prevIPs)
                    runIPs=[]
                    for run in hop['result']:
                        #hopIP="*"
                        try:
                            hopIP=run['from']
                            if not ipaddress.IPv4Address(hopIP).is_private:
                                runIPs.append(hopIP)
                                pFlag=True
                        except:
                            runIPs.append("*")

                    print(runIPs)
                    if pFlag:

                        if initialLoadPrev:
                            for ttIP in runIPs:
                                if ttIP != "*":
                                    prevIPs.add(ttIP)
                            initialLoadPrev=False
                            continue

                        newPrevIPs=set()
                        for pIP in prevIPs:
                            for rIP in runIPs:
                                if rIP=="*":
                                    rIP=pIP+"*"
                                newPrevIPs.add(rIP)
                                (a,b)=pIP,rIP
                                if a!=b:
                                    if G.has_edge(a,b):
                                        G[a][b]['weight'] += 1
                                    else:
                                        G.add_edge(a,b, weight=1)
                        prevIPs=newPrevIPs


            except KeyError:
                #print(Jdata)
                pass

            #for i in range(0,len(uniqIPs)-1):
            #    (a,b)=uniqIPs[i],uniqIPs[i+1]
            #    if G.has_edge(a,b):
            #        G[a][b]['weight'] += 1
            #    else:
            #        G.add_edge(a,b, weight=1)
        return G,dst

def processTraceroutes(lR):
    '''
    if os.path.isdir(aggResultsDir):
        resultFiles = [join(aggResultsDir, f) for f in listdir(aggResultsDir) if isfile(join(aggResultsDir, f))]
        for fname in resultFiles:
            with closing(open(fname,'r')) as fp:
                for lR in fp:
    '''
    trProcessor=tracerouteProcessor()
    eventLine=lR.split('|')
    logging.info('Processing event {0}'.format(eventLine[0]))
    pList=list(eval(eventLine[3]))
    dayStr=datetime.utcfromtimestamp(float(eventLine[1])).strftime("%Y%m%d")
    year,month,day=datetime.utcfromtimestamp(float(eventLine[1])).strftime("%Y-%m-%d").split('-')
    startTimeDate=datetime(int(year),int(month),int(day),0,0,0, tzinfo=timezone('UTC'))#.strftime('%s')
    startTimestamp = (startTimeDate - datetime(1970, 1, 1, tzinfo=timezone('UTC'))).total_seconds()
    endTime=datetime.utcfromtimestamp(float(eventLine[1])).strftime('%s')

    retTracesDict=trProcessor.getTraceroutesFromDB(pList,startTimestamp,endTime)
    try:
        for msmID,probeTraceDict in retTracesDict.items():
            preOutageDataDict={}
            G=nx.DiGraph()
            writeFlag=False
            for pID,jsonTraceList in probeTraceDict.items():
                if len(jsonTraceList)>0:
                    writeFlag=True
                    G,dst=trProcessor.toGraph(G,jsonTraceList)
            if writeFlag:
                nx.drawing.nx_agraph.write_dot(G, "topo/topo_{0}_{1}_{2}_{3}.dot".format(dayStr,eventLine[0],msmID,dst))
    except:
        traceback.print_exc()
    logging.info('Event {0} processed'.format(eventLine[0]))
    return 1

def pullTraceroutes(dictVal):
    itemsForThreads=[]
    for dayStr,probeSet in dictVal.items():
        probeIDs=[]
        year,month,day=dayStr.split('-')
        collection='traceroute_'+year+month+day
        startTime=datetime(int(year),int(month),int(day),0,0,0)
        endTime=datetime(int(year),int(month),int(day),23,59,59)
        for pid in probeSet:
            doc=trProcessor.mongodb.db[collection].find_one({"prb_id":pid})
            if doc is None:
                probeIDs.append(pid)
        if len(probeIDs)>0:
            itemsForThreads.append([collection,startTime,endTime,probeIDs])
    try:
        pool = Pool(processes=cpu_count())
        pool.map(downloader,itemsForThreads)
        pool.close()
        pool.join()
        #map(trProcessor.downloader,itemsForThreads)
    except:
        traceback.print_exc()

def downloader(item):
    print(item)
    collection,startTime,endTime,probeIDs=item
    mongodb=mongoClient()
    msmIDs=[]
    files=['data/anchorMsmIdsv4.txt','data/builtinMsmIdsv4.txt']
    #files=['data/builtinMsmIdsv4.txt']
    for file in files:
        with open(file,'r') as fp:
            for line in fp:
                l=int(line.rstrip('\n').split(':')[1])
                msmIDs.append(l)
    '''
    if self.USE_STREAM:
        try:

            #Read Stream
            atlas_stream = AtlasStream()
            atlas_stream.connect()

            # Probe's connection status results
            channel = "result"

            atlas_stream.bind_channel(channel, self.onTracerouteResponse)

            #for msm in msmIDs:
            #print(msm)
            stream_parameters = {"msm": msm,"startTime":startTime,"endTime":endTime}
            atlas_stream.start_stream(stream_type="result", **stream_parameters)

            atlas_stream.timeout()

            # Shut down everything
            atlas_stream.disconnect()
        except:
            print('Unexpected Event. Quiting.')
            atlas_stream.disconnect()
    else:
    '''
    #startTime=datetime.fromtimestamp(1461911358.0)
    #endTime=datetime.fromtimestamp(1461912358.5)
    for msm in msmIDs:
        try:
            kwargs = {
                "msm_id": msm,
                "start":startTime,
                "stop":endTime,
                "probe_ids": probeIDs
            }

            is_success, results = AtlasResultsRequest(**kwargs).create()

            if is_success:
                if len(results)>0:
                    mongodb.insertTraceroutes(collection,results)
        except:
            traceback.print_exc()

if __name__ == "__main__":
    logging.basicConfig(filename='logs/{0}.log'.format(os.path.basename(sys.argv[0]).split('.')[0]), level=logging.INFO,\
                        format='[%(asctime)s] [%(levelname)s] %(message)s',datefmt='%m-%d-%Y %I:%M:%S')

    aggResultsDir='results/aggResults'

    #Traceroute Processor
    trProcessor=tracerouteProcessor()
    outageDayToProbeDict=trProcessor.getDayToProbesMapFromAggResults(aggResultsDir)
    #pullTraceroutes(outageDayToProbeDict)
    if os.path.isdir(aggResultsDir):
        resultFiles = [join(aggResultsDir, f) for f in listdir(aggResultsDir) if isfile(join(aggResultsDir, f))]
        for fname in resultFiles:
            with closing(open(fname,'r')) as fp:
                eventsToProcess=[]
                for lR in fp:
                    eventsToProcess.append(lR)
                logging.info('Starting Pool')
                try:
                    pool = Pool(processes=cpu_count())
                    pool.map_async(processTraceroutes,eventsToProcess)
                    pool.close()
                    pool.join()
                    #processTraceroutes(eventsToProcess[0])
                except:
                    traceback.print_exc()
                logging.info('Pool completed tasks')
    logging.info('Finished processing traceroutes')

