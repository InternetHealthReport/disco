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


def isTraceComplete(traceList):
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
            return True

    return False

def calcTR(numberOfTraceroutes,numberOfProbes,outageDuration):
    if numberOfTraceroutes==0:
        return 0
    elif numberOfProbes==0:
        return 0
    else:
        return float(numberOfTraceroutes/numberOfProbes/(outageDuration/60))

if __name__ == "__main__":
    logging.basicConfig(filename='logs/{0}.log'.format(os.path.basename(sys.argv[0]).split('.')[0]), level=logging.INFO,\
                        format='[%(asctime)s] [%(levelname)s] %(message)s',datefmt='%m-%d-%Y %I:%M:%S')

    pp=PrettyPrinter()
    plotter=plotter()
    plotter.suffix='Both'
    ot=outputWriter(resultfilename='outageEvalData/outageEval.txt')

    #Master trRate List
    trRateList=[]

    #Load add events
    eventsMasterDict={}
    with closing(open('results/aggResults/topEvents.txt','r')) as fp:
        for lR in fp:
            outageID,outageStart,outageEnd,probeSet,aggregation,burstID=lR.rstrip('\n').split('|')
            eventsMasterDict[int(outageID)]=[outageStart,outageEnd,eval(probeSet),aggregation,burstID]

    # Create a new tree
    geoDate='201601'
    rtree=createRadix(geoDate)

    ot.write(['outageID','trRate','percentageFailedTraceroutes','percentageCouldPredictNextIP','problemProbes','problemPrefixes','problemAS'])

    trResultsDir='tracerouteAnalysisResults/'
    if os.path.isdir(trResultsDir):
        picFiles = [join(trResultsDir, f) for f in listdir(trResultsDir) if isfile(join(trResultsDir, f))]
        for fname in picFiles:
            numberOfTraceroutes=0
            numberOfFailedTraceroutes=0
            numberOfTraceroutesWithValidNextIP=0
            problemProbes=set()
            problemIPs=set()
            problemPrefixes=set()
            problemAS=set()
            outageID=int(fname.split('.')[0].split('_')[1])
            outageInfo=pickle.load(open(fname,'rb'))
            outageDuration=float(eventsMasterDict[outageID][1])-float(eventsMasterDict[outageID][0])
            year,month,day=datetime.utcfromtimestamp(float(eventsMasterDict[outageID][0])).strftime("%Y-%m-%d").split('-')
            for msmID,msmTraceInfo in outageInfo.items():
                #Look at only anchoring measurement
                #if not (msmID > 5000 and msmID <= 5026):
                #if msmID > 5000 and msmID <= 5026:
                if True:
                    for probeID,probeTraceInfo in msmTraceInfo.items():
                        problemProbes.add(probeID)
                        for timeStamp,tracerouteInfo in probeTraceInfo.items():
                            numberOfTraceroutes+=1
                            tracerouteIPsWithOccuranceCounts=tracerouteInfo['traceroute']
                            failedHopInfo=tracerouteInfo['failed_hops']
                            lastFailedHop=max(failedHopInfo.keys())
                            if not isTraceComplete(tracerouteIPsWithOccuranceCounts):
                                numberOfFailedTraceroutes+=1
                                lastSeenIP=failedHopInfo[lastFailedHop]['expected_link'].keys()[0]
                                nextExpectedIP=failedHopInfo[lastFailedHop]['expected_link'][lastSeenIP]['expected_next_ip']
                                if not nextExpectedIP=='None':
                                    problemIPs.add(nextExpectedIP)
                                    numberOfTraceroutesWithValidNextIP+=1

                                    #cmd="whois {0} | grep OriginAS | awk '{1}print$2{2}'".format(nextExpectedIP,'{','}')
                                    #os.system(cmd)

            #Get probe prefixes
            pFile='probeArchiveData/'+year+'/probeArchive-'+year+month+day+'.json','r'))
            probesInfo=json.load(open(pFile))
            for probe in probesInfo:
                try:
                    prPrefix=probe['prefix_v4']
                    if prPrefix!='null':
                        problemPrefixes.add(probe['asn_v4'])
                except:
                    continue

            for pIP in problemIPs:
                rnode = rtree.search_best(pIP)
                if rnode is not None:
                    lpm=rnode.prefix
                    problemPrefixes.add(lpm)
                    asFromLookups=getOriginASes(lpm,geoDate)
                    for AS in asFromLookups:
                        problemAS.add(AS)


            trRate=calcTR(numberOfTraceroutes,len(problemProbes),outageDuration)
            trRateList.append(trRate)

            #print(fname,outageID,numberOfTraceroutes,numberOfFailedTraceroutes,numberOfTraceroutesWithValidNextIP)
            #print(problemIPs,problemAS)
            if numberOfTraceroutes==0:
                percentageFailedTraceroutes='NoTR'
            else:
                percentageFailedTraceroutes=float(numberOfFailedTraceroutes/numberOfTraceroutes*100)
            #print('Failed traceroutes: {0}%'.format(percentageFailedTraceroutes))
            if numberOfFailedTraceroutes==0:
                percentageCouldPredictNextIP='NoFTR'
            else:
                percentageCouldPredictNextIP=float(numberOfTraceroutesWithValidNextIP/numberOfFailedTraceroutes*100)

            #print('Could predict next IP: {0}%'.format(percentageCouldPredictNextIP))
            ot.write([outageID,trRate,percentageFailedTraceroutes,percentageCouldPredictNextIP,problemProbes,problemPrefixes,problemAS])

        plotter.ecdf(trRateList,'tracerouteRateDuringOutage',xlabel='Traceroute Rate',ylabel='CDF',titleInfo='Both Measurements')

