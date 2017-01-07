from __future__ import print_function,division
from contextlib import closing
import pymongo
from mongoClient import mongoClient
import sys
import glob
import json
import pandas as pd
import numpy as np
import cPickle as pickle
#import pickle as pickle
from datetime import datetime
from tracerouteProcessor import tracerouteProcessor


def trinocularAgg(duration=1800 ):
    tri = pickle.load(open("data/probePrefixOutageTrinocular.pickle","r"))
    data = []
    for k, v in tri.iteritems():
        for outage in v:
            data.append([k, outage["outageStart"], outage["outageEnd"], outage["outageEnd"]-outage["outageStart"]])

    df = pd.DataFrame(data, columns=["prefix", "start", "end", "duration"] )

    df = df[ (df["duration"]>duration) ]

    df["astart"] = df["start"]/1800
    df["aend"] = df["end"]/1800
    df["startBin"] = df["astart"].astype(int)
    df["endBin"] = df["aend"].astype(int)

    grp = df.groupby(["startBin", "endBin"])
    # The following would aggregate more bursts:
    # grp = df.groupby(["startBin"])

    outputFile = open("data/trinocularAggregatedSmall.txt", "w")
    for i, g in enumerate(grp.groups.keys()):
        event = grp.get_group(g)

        prefixes = set()
        for p in event["prefix"]:
            prefixes.add(p)

        if len(prefixes)>10:
            outputFile.write("%s|%s|%s|%s\n" % (i, event["start"].mean(), event["end"].mean(), prefixes))

    return grp



def calcTR(numberOfTraceroutes,numberOfProbes,outageDuration):
    if numberOfTraceroutes==0:
        return 0
    elif numberOfProbes==0:
        return 0
    else:
        rate=round(float(numberOfTraceroutes/(numberOfProbes*(outageDuration/60))),2)
        return rate

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

#Load MSMID to Dst map
msmIDToDstMap={}
with closing(open('data/msmIDToDst.txt')) as fp:
    for line in fp:
        mid,dstAddr=line.split(' ')
        msmIDToDstMap[int(mid)]=dstAddr.rstrip('\n')

#Read outages found by Disco
outageIDToOutageStart={}
outageIDToOutageEnd={}
discoOutageInfo={}
with closing(open('results/aggResults/topEvents.txt','r')) as fp:
    for line in fp:
        vals=line.rstrip('\n').split('|')
        outageID,outageStart,outageEnd,_,_,_=vals
        outageIDToOutageStart[outageID]=float(outageStart)
        outageIDToOutageEnd[outageID]=float(outageEnd)
dayToPrefixDisco={}
with closing(open('outageEval/outageEval30WithLen.txt','r')) as fp:
    for line in fp:
        vals=line.rstrip('\n').split('|')

        outageID,trRate,percentageFailedTraceroutes,percentageCouldPredictNextIP,\
        problemProbes,lenProblemProbes,problemProbePrefixes24,lenProblemProbePrefixes24,\
        problemPrefixes,lenProblemPrefixes,problemPrefixes24,lenProblemPrefixes24,defOutagePrefixes,\
        lenDefOutagePrefixes,defOutageProbePrefixes,lenDefOutageProbePrefixes,problemAS=vals

        if outageID!='outageID':
            dayToPrefixDisco[datetime.utcfromtimestamp(outageIDToOutageStart[outageID]).strftime("%Y%m%d")]=eval(problemProbePrefixes24)
            discoOutageInfo[int(outageID)]={}
            discoOutageInfo[int(outageID)]={'outageStart':outageIDToOutageStart[outageID],'outageEnd':outageIDToOutageEnd[outageID],'prefixes':eval(problemProbePrefixes24)}


#Read all probe prefixes
#Get probe prefixes
year='2015'
month='08'
pFile='data/probeArchiveData/'+year+'/probeArchive-'+year+month+'01'+'.json'
probePrefixes24=set()
probesInfo=json.load(open(pFile))
probeIDToCountryDict={}
probeIDToASNDict={}
prefixToProbeIDDict={}
for probe in probesInfo:
    try:
        prIP=probe['address_v4']
        if prIP!='null':
            quads=prIP.split('.')
            prefix24=quads[0]+'.'+quads[1]+'.'+quads[2]+'.'+'0/24'
            probePrefixes24.add(prefix24)
            prefixToProbeIDDict[prefix24]=probe['id']
            asn=probe['asn_v4']
            if asn is not None and asn != "" and asn != 'None':
                probeIDToASNDict[probe['id']]=asn
            country=probe['country_code']
            if country is not None and country != "" and country != 'None':
                probeIDToCountryDict[probe['id']]=country

    except:
        continue

#print(len(probePrefixes24))

#MongoDB
mongodb=mongoClient()
#Traceroute Processor
tracerouteProcessor=tracerouteProcessor()

'''
#Collections
#collections=['pingoutage_20150609','pingoutage_20150608','pingoutage_20151204',\
#'pingoutage_20151205','pingoutage_20151102','pingoutage_20151203','pingoutage_20151201','pingoutage_20150601','pingoutage_20150603','pingoutage_20150602',\
#'pingoutage_20150605','pingoutage_20150604','pingoutage_20150607','pingoutage_20150606','pingoutage_20151128','pingoutage_20151207',\
#'pingoutage_20151202','pingoutage_20151227','pingoutage_20150713','pingoutage_20150918','pingoutage_20151226','pingoutage_20150831',\
#'pingoutage_20150528','pingoutage_20150529','pingoutage_20150905','pingoutage_20150520','pingoutage_20150521','pingoutage_20150522']



# print(len(collections))

#Check which probe prefixes suffered an outage in Trinocular

collections=mongodb.getCollectionsLike('pingoutage_')
#outageInfo
prefixOutageTimeDict={}
for prf in probePrefixes24:
    for collc in collections:
        print(collc)
        listOfdefBlocksOutages=mongodb.getPingOutagesByDuration(prf,collc,30*60)
        #for blockPrf in listOfdefBlocksOutages:
        #    defOutageProbePrefixes.add(blockPrf)
        if len(listOfdefBlocksOutages)>0:
            for dict in listOfdefBlocksOutages:
                prf=dict['outagePrefix']
                if prf not in prefixOutageTimeDict.keys():
                    prefixOutageTimeDict[prf]=[]
                prefixOutageTimeDict[prf].append({'outageStart':dict['outageStart'],'outageEnd':dict['outageEnd']})

pickle.dump(prefixOutageTimeDict,open('probePrefixOutageTrinocular.pickle','wb'))
'''
# data/trinocularAggregated.txt from Romains script
#trinocularAgg()
#collections=['traceroute_2015_12_05','traceroute_2015_12_04','traceroute_2015_12_07','traceroute_2015_12_06',\
#             'traceroute_2015_12_01' ,'traceroute_2015_12_26','traceroute_2015_12_03','traceroute_2015_12_02',\
#             'traceroute_2015_12_29','traceroute_2015_12_28','traceroute_2015_12_09','traceroute_2015_12_08',\
#            'traceroute_2015_12_23','traceroute_2015_12_22','traceroute_2015_12_21']

#prefixOutageTimeDict=pickle.load(open('data/probePrefixOutageTrinocular.pickle','rb'))
outageMasterDict={}
with closing(open('data/trinocularAggregatedSmall.txt','r')) as fp:
    for lineR in fp:
        line=lineR.rstrip('\n')
        outageID,outageStart,outageEnd,prefixes=line.split('|')
        outageMasterDict[outageID]={'prefixes':eval(prefixes),'outageStart':float(outageStart),'outageEnd':float(outageEnd)}
WINDOW=1*60*60
outageMaster2={}
trinocularOutages={}
for otID,prefixesDict in outageMasterDict.items():
    thisAgg={}
    #Check aggregation
    prefixes=prefixesDict['prefixes']
    for prefEntry in prefixes:
        try:
            asn=probeIDToASNDict[prefixToProbeIDDict[prefEntry]]
        except:
            continue
        if asn not in thisAgg.keys():
            thisAgg[asn]=[]
        f=True
        for vls in thisAgg[asn]:
            if vls==prefEntry:
                f=False
        if f:
            thisAgg[asn].append(prefEntry)
        country=probeIDToCountryDict[prefixToProbeIDDict[prefEntry]]
        if country not in thisAgg.keys():
            thisAgg[country]=[]
        f=True
        for vls in thisAgg[country]:
            if vls==prefEntry:
                f=False
        if f:
            thisAgg[asn].append(prefEntry)

    seenByDiscoToo={}
    oStart=outageMasterDict[otID]['outageStart']
    oEnd=outageMasterDict[otID]['outageEnd']
    for agg,prefixesInAgg in thisAgg.items():
        if len(prefixesInAgg)>=10:
            for prfs in prefixesInAgg:
                if otID not in trinocularOutages.keys():
                    trinocularOutages[otID]={'prefixes':set(),'outageStart':oStart,'outageEnd':oEnd}
                trinocularOutages[otID]['prefixes'].add(prfs)
            for doid,doDict in discoOutageInfo.items():
                if abs(doDict['outageStart']-oStart)<=WINDOW:# and abs(doDict['outageEnd']-oEnd)<=WINDOW:
                    #print(doDict['outageStart'],oStart)
                    #print(abs(doDict['outageStart']-oStart))
                    for prfs in prefixesInAgg:
                        if prfs in doDict['prefixes']:
                            if otID not in seenByDiscoToo.keys():
                                seenByDiscoToo[otID]={'prefixes':set()}
                            seenByDiscoToo[otID]['prefixes'].add(prfs)

print('Trinocular outages after filters: '+str(len(trinocularOutages)))
#print(discoOutageInfo)
print('Trinocular outages also seen by disco: '+str(len(seenByDiscoToo)))

for otID,outageDictInfo in trinocularOutages.items():
    outageStart=outageDictInfo['outageStart']
    outageEnd=outageDictInfo['outageEnd']
    year,month,day=datetime.utcfromtimestamp(float(outageStart)).strftime("%Y-%m-%d").split('-')
    if year != '2015' or month != '12':
        continue
    collec='traceroute'+'_'+year+'_'+month+'_'+day
    #print(year,month)

    print(otID,collec)
    sys.stdout.flush()
    outageDuration=outageEnd-outageStart
    probeSet=set()
    for prf in outageDictInfo['prefixes']:
        try:
            probeSet.add(prefixToProbeIDDict[prf])
        except:
            pass
    numberOfTraceroutes=0
    numberOfFailedTraceroutes=0
    numberOfSuccessfulTraceroutes=0
    numberOfTraceroutesWithValidNextIP=0
    numberOfTraceroutesB4=0
    numberOfFailedTraceroutesB4=0
    numberOfSuccessfulTraceroutesB4=0
    numberOfTraceroutesAf=0
    numberOfFailedTraceroutesAf=0
    numberOfSuccessfulTraceroutesAf=0
    windowDuration=(1*60*60)
    for msmID in tracerouteProcessor.msmIDs:
        #Look at only anchoring measurement
        #if not (msmID > 5000 and msmID <= 5026):
        #if msmID > 5000 and msmID <= 5026:
        if True:
            #for ppID in probeSet:
            ppIDList=list(probeSet)
            try:
                traceroutesBeforeOutage=mongodb.getTraceroutesAtlasDB(outageStart-windowDuration,outageStart,ppIDList,msmID)
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
                            if not isTraceComplete(msmID,ipsSeen):
                                numberOfFailedTraceroutesB4+=1
                            else:
                                numberOfSuccessfulTraceroutesB4+=1
                        except:
                            pass

                traceroutesBeforeOutage=mongodb.getTraceroutesAtlasDB(outageStart,outageEnd,ppIDList,msmID)
                if len(traceroutesBeforeOutage)>0:
                    for Jdata in traceroutesBeforeOutage:
                        numberOfTraceroutes+=1
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
                            if not isTraceComplete(msmID,ipsSeen):
                                numberOfFailedTraceroutes+=1
                            else:
                                numberOfSuccessfulTraceroutes+=1
                        except:
                            pass

                traceroutesBeforeOutage=mongodb.getTraceroutesAtlasDB(outageEnd,outageEnd+windowDuration,ppIDList,msmID)
                if len(traceroutesBeforeOutage)>0:
                    for Jdata in traceroutesBeforeOutage:
                        numberOfTraceroutesAf+=1
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
                            if not isTraceComplete(msmID,ipsSeen):
                                numberOfFailedTraceroutesAf+=1
                            else:
                                numberOfSuccessfulTraceroutesAf+=1
                        except:
                            pass
            except:
                traceback.print_exc()

    print(numberOfSuccessfulTraceroutesB4,numberOfSuccessfulTraceroutes,numberOfSuccessfulTraceroutesAf,len(probeSet))
    trRateB4Succ=calcTR(numberOfSuccessfulTraceroutesB4,len(probeSet),windowDuration)
    trRateAfSucc=calcTR(numberOfSuccessfulTraceroutesAf,len(probeSet),windowDuration)
    trrRefSucc='NA'
    if trRateAfSucc!=0:
        trrRefSucc=float("{0:.2f}".format((trRateB4Succ/trRateAfSucc)))

    trRateSucc=calcTR(numberOfSuccessfulTraceroutes,len(probeSet),outageDuration)
    trRateOtSucc=calcTR((numberOfSuccessfulTraceroutesAf+numberOfSuccessfulTraceroutesB4),len(probeSet),2*windowDuration)
    trrCalcSucc='NA'
    if trRateOtSucc!=0:
        trrCalcSucc=float("{0:.2f}".format((trRateSucc/trRateOtSucc)))

    trRateB4Fail=calcTR(numberOfFailedTraceroutesB4,len(probeSet),windowDuration)
    trRateAfFail=calcTR(numberOfFailedTraceroutesAf,len(probeSet),windowDuration)
    trrRefFail='NA'
    if trRateAfFail!=0:
        trrRefFail=float("{0:.2f}".format((trRateB4Fail/trRateAfFail)))


    trRateFail=calcTR(numberOfFailedTraceroutes,len(probeSet),outageDuration)
    trRateOtFail=calcTR((numberOfFailedTraceroutesAf+numberOfFailedTraceroutesB4),len(probeSet),2*windowDuration)
    trrCalcFail='NA'
    if trRateOtFail!=0:
        trrCalcFail=float("{0:.2f}".format((trRateFail/trRateOtFail)))

    with closing(open('data/trinocularValidations.txt','a')) as fp:
        print(otID,trRateSucc,trRateOtSucc,trrRefSucc,trrCalcSucc,trRateFail,trRateOtFail,trrRefFail,trrCalcFail,file=fp)

    print('Done.')