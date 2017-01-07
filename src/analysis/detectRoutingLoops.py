from __future__ import division,print_function
import pickle
import logging
import os
import json
from datetime import datetime
from os import listdir
from os.path import isfile, join
import sys
from pprint import PrettyPrinter
from contextlib import closing
from ip2as import *
import threading
import traceback
import csv
import numpy as np
from tracerouteProcessor import tracerouteProcessor
from plotFunctions import plotter


class outputWriter():
    def __init__(self,resultfilename=None):
        if not resultfilename:
            print('Please give a result filename.')
            exit(0)
        self.lock = threading.RLock()
        self.resultfilename = resultfilename
        if os.path.exists(self.resultfilename):
            os.remove(self.resultfilename)

    def write(self,val,delimiter="|"):
        self.lock.acquire()
        try:
            with closing(open(self.resultfilename, 'a+')) as csvfile:
                writer = csv.writer(csvfile, delimiter=delimiter)
                writer.writerow(val)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()


def isTraceComplete(msmID,traceList):
    if len(traceList)==0:
        return False
    #Check if last IP present or not
    lastHop=dict(traceList[-1]).keys()
    allIPsInTrace=set()
    for it in range(0,len(traceList)-1):
        tr=traceList[it]
        for ip in tr.keys():
            allIPsInTrace.add(ip)
    if len(lastHop) > 0:
        if lastHop[0] not in allIPsInTrace:
            #print(msmIDToDstMap[msmID],lastHop[0])
            if msmID in msmIDToDstMap.keys():
                if msmIDToDstMap[msmID]==lastHop[0]:
                    return True
            else:
                return True

    return False

def hasLoop(traceList):
    #1: has lop, 0: no loop, -1:NA
    loopIPs=[]
    lastSeenIP=None
    if len(traceList)==0:
        return -1
    #print(traceList)
    #Check if last IP present or not
    lastHop=dict(traceList[-1]).keys()
    validIPlastSeen=None
    allIPsInTrace=set()
    for it in range(0,len(traceList)):
        tr=traceList[it]
        for ip in tr.keys():
            if ip !="":
                if ip in allIPsInTrace:
                    loopIPs=[lastSeenIP,ip]
                    return 1,loopIPs
                else:
                    allIPsInTrace.add(ip)
                    lastSeenIP=ip
    return 0,loopIPs

if __name__ == "__main__":

    logging.basicConfig(filename='logs/{0}.log'.format(os.path.basename(sys.argv[0]).split('.')[0]), level=logging.INFO,\
                        format='[%(asctime)s] [%(levelname)s] %(message)s',datefmt='%m-%d-%Y %I:%M:%S')

    pp=PrettyPrinter()
    ot=outputWriter(resultfilename='outageEval/routingLoopDetector.txt')
    plotter=plotter()
    plotter.suffix='Both'
    #Traceroute Processor
    tracerouteProcessor=tracerouteProcessor()

    #Load add events
    eventsMasterDict={}
    with closing(open('results/aggResults/topEvents.txt','r')) as fp:
        for lR in fp:
            outageID,outageStartStr,outageEndStr,probeSet,aggregation,burstID=lR.rstrip('\n').split('|')
            eventsMasterDict[int(outageID)]=[outageStartStr,outageEndStr,eval(probeSet),aggregation,burstID]

    #Load MSMID to Dst map
    msmIDToDstMap={}
    with closing(open('data/msmIDToDst.txt')) as fp:
        for line in fp:
            mid,dstAddr=line.split(' ')
            msmIDToDstMap[int(mid)]=dstAddr.rstrip('\n')

    ## Create a new tree
    #geoDate='201601'
    #rtree=createRadix(geoDate)

    ot.write(['outageID','percentageTraceroutesWithRoutingLoop'])
    loopdata=[]
    loopIPsMap={}
    sameLoopCounts=[]
    trResultsDir='tracerouteAnalysisResults/'
    numberOfLoopTraceroutesPerOutage={}
    measurementsThatHaveLoops=set()
    if os.path.isdir(trResultsDir):
        picFiles = [join(trResultsDir, f) for f in listdir(trResultsDir) if isfile(join(trResultsDir, f))]
        for fname in picFiles:
            numberOfTraceroutes=0
            numberOfFailedTraceroutes=0
            numberOfLoopTraceroutes=0
            outageID=int(fname.split('.')[0].split('_')[1])
            outageInfo=pickle.load(open(fname,'rb'))
            outageStart=float(eventsMasterDict[outageID][0])
            outageEnd=float(eventsMasterDict[outageID][1])
            outageDuration=outageEnd-outageStart
            year,month,day=datetime.utcfromtimestamp(float(eventsMasterDict[outageID][0])).strftime("%Y-%m-%d").split('-')
            print('Processing outage: {0}'.format(outageID))
            #print('Outage year: {0}'.format(year))
            sys.stdout.flush()
            probeSet=eventsMasterDict[outageID][2]
            #print(year,month,day)
            #sys.stdout.flush()
            for msmID,msmTraceInfo in outageInfo.items():

                localnumberOfTraceroutes=0
                localnumberOfLoopTraceroutes=0
                #Look at only anchoring measurement
                #if not (msmID > 5000 and msmID <= 5026):
                #if msmID > 5000 and msmID <= 5026:
                if True:
                    for probeID,probeTraceInfo in msmTraceInfo.items():
                        for timeStamp,tracerouteInfo in probeTraceInfo.items():
                            numberOfTraceroutes+=1
                            localnumberOfTraceroutes+=1
                            tracerouteIPsWithOccuranceCounts=tracerouteInfo['traceroute']
                            failedHopInfo=tracerouteInfo['failed_hops']
                            lastFailedHop=max(failedHopInfo.keys())
                            if not isTraceComplete(msmID,tracerouteIPsWithOccuranceCounts):
                                numberOfFailedTraceroutes+=1
                                isLoop,loopIPsList=hasLoop(tracerouteIPsWithOccuranceCounts)
                                if isLoop==1:
                                    measurementsThatHaveLoops.add(msmID)
                                    numberOfLoopTraceroutes+=1
                                    if outageID not in numberOfLoopTraceroutesPerOutage.keys():
                                        numberOfLoopTraceroutesPerOutage[outageID]=1
                                    else:
                                        numberOfLoopTraceroutesPerOutage[outageID]+=1
                                    localnumberOfLoopTraceroutes+=1
                                    k=str(loopIPsList[0])+str(loopIPsList[1])
                                    kprime=str(loopIPsList[1])+str(loopIPsList[0])
                                    if outageID not in loopIPsMap.keys():
                                        loopIPsMap[outageID]=set()
                                    if kprime not in loopIPsMap[outageID]:
                                        loopIPsMap[outageID].add(k)
            print(outageID,numberOfLoopTraceroutes,numberOfFailedTraceroutes,numberOfTraceroutes)
            if numberOfTraceroutes>0:
                trLoop=(numberOfLoopTraceroutes/numberOfFailedTraceroutes*100)
                percentageTraceroutesWithRoutingLoop=float("{0:.2f}".format(trLoop))
                ot.write([outageID,percentageTraceroutesWithRoutingLoop])
                loopdata.append(percentageTraceroutesWithRoutingLoop)

        for otID,setOfLoopIPs in loopIPsMap.items():
            sameLoopCounts.append(len(setOfLoopIPs)/numberOfLoopTraceroutesPerOutage[otID]*100)
        #loopdata=[45.91, 11.75, 0.0, 0.0, 0.0, 14.29, 15.0, 3.78, 0.0, 73.25, 22.0, 45.74, 20.0, 12.73, 20.41, 17.78, \
        #          24.02, 18.26, 25.0, 0.0, 15.79, 14.58, 0.93, 1.31, 0.0, 0.62, 0.0, 50.0, 1.33, 5.71, 14.71, 6.98,\
        #          33.33, 24.67, 4.74, 33.33, 9.54, 18.75, 14.58, 9.09, 15.63, 1.58, 11.43, 1.33, 9.57, 6.9, 32.09,\
        #          9.71, 0.0, 13.12, 17.06, 14.29, 7.1, 16.1, 4.71, 33.33, 0.0, 5.11, 2.15, 30.14, 74.55, 6.25, 1.26,\
        #          1.75, 0.0, 0.0, 1.5, 24.71, 6.31, 4.28, 5.84, 31.25, 28.54, 0.0, 11.37]
        plotter.ecdf(loopdata,'percentageRoutingLoops',xlabel='Percentage of traceroutes with loops',ylabel='CDF: #Outages')
        plotter.ecdf(sameLoopCounts,'sameRoutingLoops',xlabel='Percentage of traceroutes with same loop',ylabel='CDF: #Outages')


        print('Done.')

