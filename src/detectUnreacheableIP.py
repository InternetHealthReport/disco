from ripe.atlas.cousteau import AtlasResultsRequest
from mongoClient import mongoClient
from datetime import datetime,timedelta
from pytz import timezone
import operator
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
from collections import OrderedDict
import networkx as nx
import pickle
import ipaddress
from tracerouteProcessor import tracerouteProcessor

def getExpectedIP(nextNodes):
    ip='None'
    prob='None'
    if len(nextNodes)>0:
        #Calculate Probabilities
        #Get total
        total=0
        for v in nextNodes.values():
            total+=float(v["weight"])

        probabilityDict={}
        for ip,ipinfo in nextNodes.items():
            probabilityDict[ip]=float(ipinfo["weight"])/total

        sortedProbDict=OrderedDict(sorted(probabilityDict.items(), key=operator.itemgetter(1),reverse=True))
        ip=sortedProbDict.keys()[0]
        prob=sortedProbDict.values()[0]
    return ip,prob

def runAnalysis(lR):
    pp=pprint.PrettyPrinter()
    trProcessor=tracerouteProcessor()
    eventLine=lR.split('|')
    #logging.info('Processing event {0}'.format(eventLine[0]))
    pList=list(eval(eventLine[3]))
    dayStr=datetime.utcfromtimestamp(float(eventLine[1])).strftime("%Y%m%d")
    #year,month,day=datetime.utcfromtimestamp(float(eventLine[1])).strftime("%Y-%m-%d").split('-')
    startTime=float(eventLine[1])
    endTime=float(eventLine[2])
    #DOT files
    topoDir='topo/'
    dotFiles = [join(topoDir, f) for f in listdir(topoDir) if isfile(join(topoDir, f))]

    retTracesDict=trProcessor.getTraceroutesFromDB(pList,startTime,endTime)
    try:
        hopNumToExpectedIP={}
        for msmID,probeTraceDict in retTracesDict.items():

            dotFile=None
            dotFileStr='topo/topo_{0}_{1}_{2}'.format(dayStr,eventLine[0],msmID)
            for fname in dotFiles:
                #print(dotFileStr,fname)
                if dotFileStr in fname:
                    dotFile=fname
                    break
            if dotFile is None:
                continue
            #print('Loading {0}'.format(dotFile))
            gModel=nx.DiGraph(nx.drawing.nx_agraph.read_dot(dotFile))
            #G=nx.DiGraph()

            for pID,jsonTraceList in probeTraceDict.items():
                if len(jsonTraceList)>0:

                    #G,dst=trProcessor.toGraph(G,jsonTraceList)
                    for Jdata in jsonTraceList:
                        try:
                            hops=Jdata['result']
                            trTimestamp=Jdata['timestamp']
                            #Populate all hops in this traceroute
                            ipsSeen=[]
                            for hop in hops:
                                thisRunIPs={}
                                for run in hop['result']:
                                    try:
                                        hopIP=run['from']
                                        if hopIP not in thisRunIPs:
                                            thisRunIPs[hopIP]=1
                                        else:
                                            thisRunIPs[hopIP]+=1
                                    except:
                                        #print(hops)
                                        continue

                                ipsSeen.append(thisRunIPs)
                            prevIPs=[]
                            for hop in hops:
                                hopNum=hop["hop"]
                                hopIP="*"
                                thisRunIPs={}
                                for run in hop['result']:
                                    try:
                                        hopIP=run['from']
                                        if hopIP not in thisRunIPs:
                                            thisRunIPs[hopIP]=1
                                        else:
                                            thisRunIPs[hopIP]+=1
                                    except:
                                        #print(hops)
                                        continue

                                #ipsSeen.append(thisRunIPs)
                                if len(thisRunIPs)==0 and len(prevIPs)>0:
                                    expectedLinkDict={}
                                    for pIP in prevIPs:
                                        recursiveFlag=False
                                        nextExpectedHopIP='None'
                                        prob='None'
                                        try:
                                            nextExpectedHopIP,prob=getExpectedIP(gModel[pIP])
                                            while("*" in nextExpectedHopIP):
                                                nextExpectedHopIP,prob=getExpectedIP(gModel[nextExpectedHopIP])
                                                recursiveFlag=True
                                        except:
                                            pass

                                        if pIP not in expectedLinkDict.keys():
                                            expectedLinkDict[pIP]={"expected_next_ip":nextExpectedHopIP,"probability":prob,"recursive":recursiveFlag,"count":1}
                                        else:
                                            expectedLinkDict[pIP]["count"]+=1


                                    if len(expectedLinkDict)>0:
                                        if msmID not in hopNumToExpectedIP.keys():
                                            hopNumToExpectedIP[msmID]={}
                                        if pID not in hopNumToExpectedIP[msmID].keys():
                                            hopNumToExpectedIP[msmID][pID]={}
                                        if trTimestamp not in hopNumToExpectedIP[msmID][pID].keys():
                                            hopNumToExpectedIP[msmID][pID][trTimestamp]={}
                                            hopNumToExpectedIP[msmID][pID][trTimestamp]["traceroute"]=ipsSeen
                                            hopNumToExpectedIP[msmID][pID][trTimestamp]["failed_hops"]={}
                                        if hopNum not in hopNumToExpectedIP[msmID][pID][trTimestamp]["failed_hops"].keys():
                                            hopNumToExpectedIP[msmID][pID][trTimestamp]["failed_hops"][hopNum]={}
                                        hopNumToExpectedIP[msmID][pID][trTimestamp]["failed_hops"][hopNum]={"expected_link":expectedLinkDict}
                                    #break
                                #else:
                                try:
                                    del prevIPs[:]
                                    for hIP,count in thisRunIPs.items():
                                        for ite in range(0,count):
                                            if not ipaddress.IPv4Address(hIP.decode('utf-8')).is_private:
                                                prevIPs.append(hIP)
                                except:
                                    pass

                        except:
                            #print(Jdata)
                            #traceback.print_exc()
                            pass
                #else:
                #    print('No traceroute during outage!')
        pickle.dump(hopNumToExpectedIP,open('tracerouteAnalysisResults/expectedUnreacheableIPs_{0}.pickle'.format(eventLine[0]),'wb'))
    except:
        traceback.print_exc()

if __name__ == "__main__":
    logging.basicConfig(filename='logs/{0}.log'.format(os.path.basename(sys.argv[0]).split('.')[0]), level=logging.INFO,\
                        format='[%(asctime)s] [%(levelname)s] %(message)s',datefmt='%m-%d-%Y %I:%M:%S')

    aggResultsDir='results/aggResults'
    if os.path.isdir(aggResultsDir):
        resultFiles = [join(aggResultsDir, f) for f in listdir(aggResultsDir) if isfile(join(aggResultsDir, f))]
        for fname in resultFiles:
            with closing(open(fname,'r')) as fp:
                eventsToProcess=[]
                for lR in fp:
                    eventsToProcess.append(lR.rstrip('\n'))
                try:

                    pool = Pool(processes=cpu_count())
                    pool.map(runAnalysis,eventsToProcess)
                    pool.close()
                    pool.join()
                    #map(runAnalysis,eventsToProcess)
                    #runAnalysis(eventsToProcess[2])
                except:
                    traceback.print_exc()
    logging.info('Finished processing')