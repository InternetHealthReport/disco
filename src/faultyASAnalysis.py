from __future__ import division,print_function
import pickle
import os
from datetime import datetime
from os import listdir
from os.path import isfile, join
import sys
from pprint import PrettyPrinter
from contextlib import closing
from ip2as import *
from plotFunctions import plotter
import json

def isCustomer(asn1,asn2):
    if asn2 in coneDict.keys():
        if asn1 in coneDict[asn2]:
            return 1

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

if __name__ == "__main__":
    #Load add events
    eventsMasterDict={}
    with closing(open('results/aggResults/topEvents.txt','r')) as fp:
        for lR in fp:
            outageID,outageStartStr,outageEndStr,probeSet,aggregation,burstID=lR.rstrip('\n').split('|')
            eventsMasterDict[int(outageID)]=[outageStartStr,outageEndStr,eval(probeSet),aggregation,burstID]
    year,month,day=datetime.utcfromtimestamp(float(eventsMasterDict[int(outageID)][0])).strftime("%Y-%m-%d").split('-')
    #Load MSMID to Dst map
    msmIDToDstMap={}
    with closing(open('data/msmIDToDst.txt')) as fp:
        for line in fp:
            mid,dstAddr=line.split(' ')
            msmIDToDstMap[int(mid)]=dstAddr.rstrip('\n')

    plotter=plotter()
    # Create a new tree
    geoDate='201601'
    rtree=createRadix(geoDate)

    coneDict={}
    with closing(open('data/as-coneases-20160901.txt','r')) as fp:
        for line in fp:
            vals=line.rstrip('\n').split(' ')
            coneDict[int(vals[0])]=set()
            for asn in vals[1:]:
                coneDict[int(vals[0])].add(int(asn))

    pFile='data/probeArchiveData/'+year+'/probeArchive-'+year+month+'01'+'.json'
    probesInfo=json.load(open(pFile))

    labels=['InterAS','ProviderAS','ProvidersProviderAS','NonProviderAS']
    InterAS=0
    ProviderAS=0
    ProvidersProviderAS=0
    NonProviderAS=0

    trResultsDir='tracerouteAnalysisResults/'
    if os.path.isdir(trResultsDir):
        picFiles = [join(trResultsDir, f) for f in listdir(trResultsDir) if isfile(join(trResultsDir, f))]
        for fname in picFiles:
            numberOfTraceroutes=0
            numberOfFailedTraceroutes=0
            numberOfSuccessfulTraceroutes=0
            numberOfTraceroutesWithValidNextIP=0

            problemProbes=set()

            problemIPs=set()
            problemPrefixes=set()
            problemPrefixes24=set()

            problemProbePrefixes=set()
            problemProbePrefixes24=set()
            problemProbeIPs=set()

            problemAS=set()
            problemProbeAS=set()

            problemIPToProbeMap={}
            problemPrefix24ToProbeMap={}

            ip2ASMap={}


            toPlotRatioFaultyIPsPerProbe=[]
            defOutagePrefixes=set()
            defOutageProbePrefixes=set()
            outageID=int(fname.split('.')[0].split('_')[1])
            outageInfo=pickle.load(open(fname,'rb'))
            outageStart=float(eventsMasterDict[outageID][0])
            outageEnd=float(eventsMasterDict[outageID][1])
            outageDuration=outageEnd-outageStart
            year,month,day=datetime.utcfromtimestamp(float(eventsMasterDict[outageID][0])).strftime("%Y-%m-%d").split('-')
            #print('Processing outage: {0}'.format(outageID))
            #print('Outage year: {0}'.format(year))
            sys.stdout.flush()
            probeSet=eventsMasterDict[outageID][2]
            #print(year,month,day)
            #sys.stdout.flush()
            for msmID,msmTraceInfo in outageInfo.items():
                #Look at only anchoring measurement
                #if not (msmID > 5000 and msmID <= 5026):
                #if msmID > 5000 and msmID <= 5026:
                if True:
                    for probeID,probeTraceInfo in msmTraceInfo.items():
                        problemProbes.add(probeID)
                        prASN=None
                        for pInfo in probesInfo:
                            try:
                                if int(pInfo['id'])==int(probeID):
                                    prASN=pInfo['asn_v4']
                                    break
                            except:
                                pass
                        if prASN is None:
                            continue
                        for timeStamp,tracerouteInfo in probeTraceInfo.items():
                            numberOfTraceroutes+=1
                            tracerouteIPsWithOccuranceCounts=tracerouteInfo['traceroute']
                            failedHopInfo=tracerouteInfo['failed_hops']
                            lastFailedHop=max(failedHopInfo.keys())
                            if not isTraceComplete(msmID,tracerouteIPsWithOccuranceCounts):
                                numberOfFailedTraceroutes+=1
                                lastSeenIP=failedHopInfo[lastFailedHop]['expected_link'].keys()[0]
                                nextExpectedIP=failedHopInfo[lastFailedHop]['expected_link'][lastSeenIP]['expected_next_ip']
                                if not nextExpectedIP=='None':
                                    problemIPs.add(nextExpectedIP)
                                    if nextExpectedIP not in ip2ASMap.keys():
                                        rnode = rtree.search_best(nextExpectedIP)
                                        if rnode is not None:
                                            lpm=rnode.prefix
                                            problemPrefixes.add(lpm)
                                            asFromLookups=getOriginASes(lpm,geoDate)
                                            for AS in asFromLookups:
                                                problemAS.add(AS)
                                                ip2ASMap[nextExpectedIP]=AS
                                    try:
                                        nextAS=ip2ASMap[nextExpectedIP]
                                    except:
                                        continue

                                    asn1=int(prASN)
                                    asn2=int(nextAS)

                                    #print(asn1,asn2)

                                    if asn1==asn2:
                                        InterAS+=1
                                    if isCustomer(asn1,asn2):
                                        ProviderAS+=1
                                    else:
                                        flag=False
                                        providersProvider=set()
                                        providers=set()
                                        #get providers
                                        for asn in coneDict.keys():
                                            if asn1 in coneDict[asn]:
                                                providers.add(asn)
                                        #get provider's provider
                                        for asn in coneDict.keys():
                                            for pasn in providers:
                                                if pasn in coneDict[asn]:
                                                    providersProvider.add(asn)

                                        for asn3 in providersProvider:
                                            if asn2==asn3:
                                                ProvidersProviderAS+=1
                                                flag=True
                                        if not flag:
                                            NonProviderAS+=1
                                    numberOfTraceroutesWithValidNextIP+=1
                            else:
                                numberOfSuccessfulTraceroutes+=1

    print(labels,[InterAS,ProviderAS,ProvidersProviderAS,NonProviderAS])
    print([InterAS,ProviderAS,ProvidersProviderAS,NonProviderAS])
    plotter.suffix='Both'
    plotter.plotPie(labels,[InterAS,ProviderAS,ProvidersProviderAS,NonProviderAS],(0, 0.1, 0.1, 0),'pieChartASWhereTRFailed')

    #['InterAS', 'ProviderAS', 'ProvidersProviderAS', 'NonProviderAS'] [6074, 7584, 19, 1727]
