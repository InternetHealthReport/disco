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
from mongoClient import mongoClient

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
        rate=round(float(numberOfTraceroutes/numberOfProbes/(outageDuration/60)),4)
        retRate=round(rate,2)
        #print(numberOfTraceroutes,numberOfProbes,outageDuration)
        #print('Rate: {0}'.format(rate))
        #print('retRate: {0}'.format(retRate))
        #sys.stdout.flush()
        return retRate

if __name__ == "__main__":
    logging.basicConfig(filename='logs/{0}.log'.format(os.path.basename(sys.argv[0]).split('.')[0]), level=logging.INFO,\
                        format='[%(asctime)s] [%(levelname)s] %(message)s',datefmt='%m-%d-%Y %I:%M:%S')

    pp=PrettyPrinter()
    plotter=plotter()
    plotter.suffix='Both'
    ot=outputWriter(resultfilename='outageEval/outageEval11WithLen.txt')

    #Master trRate List
    trRateList=[]
    trRateB4List=[]
    trRatioList=[]

    #MongoDB
    mongodb=mongoClient()

    #Load add events
    eventsMasterDict={}
    with closing(open('results/aggResults/topEvents.txt','r')) as fp:
        for lR in fp:
            outageID,outageStartStr,outageEndStr,probeSet,aggregation,burstID=lR.rstrip('\n').split('|')
            eventsMasterDict[int(outageID)]=[outageStartStr,outageEndStr,eval(probeSet),aggregation,burstID]

    # Create a new tree
    geoDate='201601'
    rtree=createRadix(geoDate)

    ot.write(['outageID','trRate','trRateB4','trRatio','percentageFailedTraceroutes','percentageCouldPredictNextIP','problemProbes','lenProblemProbes','problemProbePrefixes24','lenProblemProbePrefixes24',\
              'problemPrefixes','lenProblemPrefixes','problemPrefixes24','lenProblemPrefixes24','defOutagePrefixes',\
              'lenDefOutagePrefixes','defOutageProbePrefixes','lenDefOutageProbePrefixes','problemAS'])

    trResultsDir='tracerouteAnalysisResults/'
    if os.path.isdir(trResultsDir):
        picFiles = [join(trResultsDir, f) for f in listdir(trResultsDir) if isfile(join(trResultsDir, f))]
        for fname in picFiles:
            numberOfTraceroutes=0
            numberOfFailedTraceroutes=0
            numberOfSuccessfulTraceroutes=0
            numberOfTraceroutesWithValidNextIP=0
            numberOfTraceroutesB4=0
            numberOfFailedTraceroutesB4=0
            numberOfSuccessfulTraceroutesB4=0
            problemProbes=set()
            problemIPs=set()
            problemPrefixes=set()
            problemPrefixes24=set()
            problemProbePrefixes=set()
            problemProbePrefixes24=set()
            problemProbeIPs=set()
            problemAS=set()
            defOutagePrefixes=set()
            defOutageProbePrefixes=set()
            outageID=int(fname.split('.')[0].split('_')[1])
            outageInfo=pickle.load(open(fname,'rb'))
            outageStart=float(eventsMasterDict[outageID][0])
            outageEnd=float(eventsMasterDict[outageID][1])
            outageDuration=outageEnd-outageStart
            year,month,day=datetime.utcfromtimestamp(float(eventsMasterDict[outageID][0])).strftime("%Y-%m-%d").split('-')
            print('Processing outage: {0}'.format(outageID))
            print('Outage year: {0}'.format(year))
            sys.stdout.flush()
            if year!='2015':
                continue
            #print(year,month,day)
            sys.stdout.flush()
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
                            else:
                                numberOfSuccessfulTraceroutes+=1
                        traceroutesBeforeOutage=mongodb.getTraceroutes(outageStart-(3*60*60),outageStart,probeID,msmID)
                        try:
                            if len(traceroutesBeforeOutage)>0:
                                for Jdata in traceroutesBeforeOutage:
                                    numberOfTraceroutesB4+=1
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
                                        if not isTraceComplete(ipsSeen):
                                            numberOfFailedTraceroutesB4+=1
                                        else:
                                            numberOfSuccessfulTraceroutesB4+=1
                                    except:
                                        pass
                        except:
                            traceback.print_exc()


            #print('Fetching probe prefixes and IPs {0}'.format(outageID))
            #sys.stdout.flush()
            #Get probe prefixes
            pFile='data/probeArchiveData/'+year+'/probeArchive-'+year+month+'01'+'.json'
            probesInfo=json.load(open(pFile))
            for probe in probesInfo:
                try:
                    #print(probe)
                    if probe['id'] in problemProbes:
                        prPrefix=(probe['prefix_v4'])
                        prIP=probe['address_v4']
                        if prPrefix!='null':
                            problemProbePrefixes.add(prPrefix)
                        if prIP!='null':
                            quads=prIP.split('.')
                            problemProbePrefixes24.add(quads[0]+'.'+quads[1]+'.'+quads[2]+'.'+'0/24')
                except:
                    continue

            #print('Populating problem prefixes {0}'.format(outageID))
            #sys.stdout.flush()
            for pIP in problemIPs:
                quads=pIP.split('.')
                problemPrefixes24.add(quads[0]+'.'+quads[1]+'.'+quads[2]+'.'+'0/24')
                rnode = rtree.search_best(pIP)
                if rnode is not None:
                    lpm=rnode.prefix
                    problemPrefixes.add(lpm)
                    asFromLookups=getOriginASes(lpm,geoDate)
                    for AS in asFromLookups:
                        problemAS.add(AS)

            #print('Fetching outage prefixes {0}'.format(outageID))
            #sys.stdout.flush()
            if year=='2015':

                for prf in problemPrefixes24:
                    listOfdefBlocksOutages=mongodb.getPingOutages(outageStart,outageEnd,prf)
                    for blockPrf in listOfdefBlocksOutages:
                        defOutagePrefixes.add(blockPrf)

                for prf in problemProbePrefixes24:
                    listOfdefBlocksOutages=mongodb.getPingOutages(outageStart,outageEnd,prf)
                    for blockPrf in listOfdefBlocksOutages:
                        defOutageProbePrefixes.add(blockPrf)

            #print(numberOfTraceroutes,numberOfTraceroutesB4)
            #print(outageStart,outageEnd,outageDuration)
            #sys.stdout.flush()
            trRate=calcTR(numberOfTraceroutes,len(problemProbes),outageDuration)
            trRateB4=calcTR(numberOfTraceroutesB4,len(problemProbes),(3*60*60))
            trRatio='NA'
            if trRateB4==0:
                trRatio=0
            else:
                trRatio=float("{0:.2f}".format((trRate/trRateB4)))
                trRateList.append(trRate)
                trRateB4List.append(trRateB4)
                trRatioList.append(trRatio)

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
            ot.write([outageID,trRate,trRateB4,trRatio,percentageFailedTraceroutes,percentageCouldPredictNextIP,problemProbes,len(problemProbes),problemProbePrefixes24,\
                      len(problemProbePrefixes24),problemPrefixes,len(problemPrefixes),problemPrefixes24,len(problemPrefixes24),\
                      defOutagePrefixes,len(defOutagePrefixes),defOutageProbePrefixes,len(defOutageProbePrefixes),problemAS])

        plotter.ecdf(trRatioList,'tracerouteRateRatio',xlabel='Traceroute Rate Ratio',ylabel='CDF',titleInfo='Both Measurements')

        print('Done.')

