
from __future__ import division,print_function
import pickle
import os
from datetime import datetime
from os import listdir
from os.path import isfile, join
import sys
import json
import numpy as np
from ip2as import *
from contextlib import closing
from pprint import PrettyPrinter
from plotFunctions import plotter
import pickle

#During outage some tracereoutes are complete, whats special about them


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

    #Load MSMID to Dst map
    msmIDToDstMap={}
    with closing(open('data/msmIDToDst.txt')) as fp:
        for line in fp:
            mid,dstAddr=line.split(' ')
            msmIDToDstMap[int(mid)]=dstAddr.rstrip('\n')

    coneDict={}
    with closing(open('data/as-coneases-20160901.txt','r')) as fp:
        for line in fp:
            vals=line.rstrip('\n').split(' ')
            coneDict[int(vals[0])]=set()
            for asn in vals[1:]:
                coneDict[int(vals[0])].add(int(asn))

    pFile='data/probeArchiveData/2016/probeArchive-20160801.json'
    probesInfo=json.load(open(pFile))
    probeIDToASNMap={}
    for pInfo in probesInfo:
        try:
            probeIDToASNMap[int(pInfo['id'])]=pInfo['asn_v4']
        except KeyError:
            #print(pInfo)
            pass
    plotter=plotter()
    # Create a new tree
    geoDate='201601'
    rtree=createRadix(geoDate)
    ip2ASMap={}
    successfullTraceroutesOutageIDToMsmID={}
    durationsOfDeltaStart=[]
    durationsOfDeltaEnd=[]
    durations=[]
    pp=PrettyPrinter()
    probeIDToCompleteDest={}
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
            print(outageID)
            sys.stdout.flush()
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
                        for timeStamp,tracerouteInfo in probeTraceInfo.items():
                            numberOfTraceroutes+=1
                            tracerouteIPsWithOccuranceCounts=tracerouteInfo['traceroute']
                            failedHopInfo=tracerouteInfo['failed_hops']
                            lastFailedHop=max(failedHopInfo.keys())

                            otStart=float(eventsMasterDict[int(outageID)][0])
                            otEnd=float(eventsMasterDict[int(outageID)][1])
                            dur=(otEnd-otStart)/60
                            durations.append(dur)


                            if not isTraceComplete(msmID,tracerouteIPsWithOccuranceCounts):
                                numberOfFailedTraceroutes+=1
                                lastSeenIP=failedHopInfo[lastFailedHop]['expected_link'].keys()[0]
                                nextExpectedIP=failedHopInfo[lastFailedHop]['expected_link'][lastSeenIP]['expected_next_ip']
                                if not nextExpectedIP=='None':
                                    problemIPs.add(nextExpectedIP)
                                    numberOfTraceroutesWithValidNextIP+=1
                            else:
                                numberOfSuccessfulTraceroutes+=1
                                diff1 = (otEnd - float(timeStamp)) / 60
                                durationsOfDeltaEnd.append(diff1)
                                diff2 = (float(timeStamp) - otStart) / 60
                                durationsOfDeltaStart.append(diff2)
                                if outageID not in successfullTraceroutesOutageIDToMsmID.keys():
                                    successfullTraceroutesOutageIDToMsmID[outageID]={}
                                if msmID not in successfullTraceroutesOutageIDToMsmID[outageID].keys():
                                    successfullTraceroutesOutageIDToMsmID[outageID][msmID]=[]
                                successfullTraceroutesOutageIDToMsmID[outageID][msmID].append(min(diff1,diff2))

                                probeAS=probeIDToASNMap[int(probeID)]
                                try:
                                    dstIP=msmIDToDstMap[msmID]
                                except:
                                    pass
                                if dstIP is None:
                                    continue
                                dstAS=None
                                if dstIP not in ip2ASMap.keys():
                                    rnode = rtree.search_best(dstIP)
                                    if rnode is not None:
                                        lpm=rnode.prefix
                                        asFromLookups=getOriginASes(lpm,geoDate)
                                        for AS in asFromLookups:
                                            dstAS=AS
                                            ip2ASMap[dstIP]=AS
                                else:
                                    dstAS=ip2ASMap[dstIP]
                                if dstAS is not None:
                                    if probeAS not in probeIDToCompleteDest.keys():
                                        probeIDToCompleteDest[probeAS]=set()
                                        if probeAS==dstAS:
                                                probeIDToCompleteDest[probeAS].add(str(dstAS)+'|self')
                                        elif probeAS in coneDict[int(dstAS)]:
                                            try:
                                                probeIDToCompleteDest[probeAS].add(str(dstAS)+'|provider')
                                            except:
                                                pass
                                        else:
                                            try:
                                                    probeIDToCompleteDest[probeAS].add(str(dstAS)+'|nonprovider')
                                            except:
                                                pass



    print('Average outage duration: '+str(np.median(durations)))
    print('Average delta start: ' + str(np.average(durationsOfDeltaStart)))
    print('Average delta end: ' + str(np.average(durationsOfDeltaEnd)))
    print('Median delta start: '+str(np.median(durationsOfDeltaStart)))
    print('Median delta end: '+str(np.median(durationsOfDeltaEnd)))
    plotter.suffix='End'
    plotter.ecdf(durationsOfDeltaEnd,'deltaDuration',xlabel='Delta from estimated outage end')
    plotter.suffix='Start'
    plotter.ecdf(durationsOfDeltaStart,'deltaDuration',xlabel='Delta from estimated outage start')
    #pp.pprint(successfullTraceroutesOutageIDToMsmID)
    pickle.dump(successfullTraceroutesOutageIDToMsmID,open('successfullTraceroutesOutageIDToMsmID.pickle','wb'))
    #pp.pprint(probeIDToCompleteDest)