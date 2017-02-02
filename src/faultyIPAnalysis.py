from __future__ import division,print_function
import matplotlib
#matplotlib.use('Agg')
#from matplotlib import pyplot as plt
import pickle
import os
from datetime import datetime
from os import listdir
from os.path import isfile, join
import sys
from contextlib import closing
from plotFunctions import plotter

#Read all tracereouteAnalysisResults
#For each outage get all estimated faulty IPs

#Probes to IP distribution


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
    filterOutageByAgg=set()
    with closing(open('results/aggResults/topEvents.txt','r')) as fp:
        for lR in fp:
            outageID,outageStartStr,outageEndStr,probeSet,aggregation,burstID=lR.rstrip('\n').split('|')
            onlyASNs=True
            for agg in eval(aggregation):
                try:
                    int(agg)
                except:
                    onlyASNs=False
            if onlyASNs:
                filterOutageByAgg.add(int(outageID))
            eventsMasterDict[int(outageID)]=[outageStartStr,outageEndStr,eval(probeSet),aggregation,burstID]

    #Load MSMID to Dst map
    msmIDToDstMap={}
    with closing(open('data/msmIDToDst.txt')) as fp:
        for line in fp:
            mid,dstAddr=line.split(' ')
            msmIDToDstMap[int(mid)]=dstAddr.rstrip('\n')

    plotter=plotter()

    toPlotRatioFaultyIPsPerProbe = []
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


            defOutagePrefixes=set()
            defOutageProbePrefixes=set()
            outageID=int(fname.split('.')[0].split('_')[1])
            #if outageID not in filterOutageByAgg:
            #    continue
            print('OutageID: '+str(outageID))
            outageInfo=pickle.load(open(fname,'rb'))
            outageStart=float(eventsMasterDict[outageID][0])
            outageEnd=float(eventsMasterDict[outageID][1])
            outageDuration=outageEnd-outageStart
            year,month,day=datetime.utcfromtimestamp(float(eventsMasterDict[outageID][0])).strftime("%Y-%m-%d").split('-')
            #print('Processing outage: {0}'.format(outageID))
            #print('Outage year: {0}'.format(year))
            #sys.stdout.flush()
            probeSet=eventsMasterDict[outageID][2]
            #print(year,month,day)
            #sys.stdout.flush()
            for msmID,msmTraceInfo in outageInfo.items():
                #Look at only anchoring measurement
                #if not (msmID > 5000 and msmID <= 5026):
                #if msmID > 5000 and msmID <= 5026:
                problemIPsInner=set()
                problemProbesInner=set()
                if True:
                    for probeID,probeTraceInfo in msmTraceInfo.items():
                        problemProbes.add(probeID)
                        problemProbesInner.add(probeID)
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
                                    problemIPsInner.add(nextExpectedIP)
                                    quads=nextExpectedIP.split('.')
                                    p24=quads[0]+'.'+quads[1]+'.'+quads[2]+'.'+'0/24'

                                    if p24 not in problemPrefix24ToProbeMap.keys():
                                        problemPrefix24ToProbeMap[p24]=set()
                                    problemPrefix24ToProbeMap[p24].add(probeID)

                                    if nextExpectedIP not in problemIPToProbeMap.keys():
                                        problemIPToProbeMap[nextExpectedIP]=set()
                                    problemIPToProbeMap[nextExpectedIP].add(probeID)

                                    numberOfTraceroutesWithValidNextIP+=1
                            else:
                                numberOfSuccessfulTraceroutes+=1
            #for faultyIP,probesDicts in problemIPToProbeMap.items():
            #    print(faultyIP,float(len(probesDicts)/len(problemProbes)*100))
            #for faultyprefix,probesDicts in problemPrefix24ToProbeMap.items():
            #   print(faultyprefix,float(len(probesDicts)/len(problemProbes)*100))
                if len(problemProbesInner)>5 and len(problemIPsInner)>0:
                    print(len(problemProbesInner), len(problemIPsInner))
                    sys.stdout.flush()
                    ratio=float(len(problemIPsInner))/len(problemProbesInner)
                    toPlotRatioFaultyIPsPerProbe.append(ratio)

    #print(toPlotRatioFaultyIPsPerProbe)

    plotter.suffix='Both'
    #print(toPlotRatioFaultyIPsPerProbe)
    plotter.ecdf(toPlotRatioFaultyIPsPerProbe,'ratioFaultyIPsPerProbe',xlabel='#Faulty IPs to #Probes',ylabel='CDF #Outages',xlim=[0,1])
    #print(toPlotRatioFaultyIPsPerProbe)
    #print(numberOfTraceroutes,numberOfFailedTraceroutes,numberOfSuccessfulTraceroutes,numberOfTraceroutesWithValidNextIP)
