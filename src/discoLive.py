from __future__ import division, print_function

import configparser
import csv
import json
import logging
import os
import sys
import threading
import time
import traceback
from contextlib import closing
import Queue
import numpy as np
import pybursts
from ripe.atlas.cousteau import AtlasStream
from choropleth import plotChoropleth
from plotFunctions import plotter
from probeEnrichInfo import probeEnrichInfo
from tracerouteProcessor import tracerouteProcessor
from pprint import PrettyPrinter
from datetime import datetime
import gzip
from os import listdir
from os.path import isfile, join

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


def on_result_response(*args):
    """
    Function that will be called every time we receive a new result.
    Args is a tuple, so you should use args[0] to access the real message.
    """
    #print args[0]
    item=args[0]
    event = eval(str(item))
    #print(event)
    dataList.append(event)
    if DETECT_DISCO_BURST:
        if event["event"] == "disconnect":
            dataQueueDisconnect.put(event)
    if DETECT_CON_BURST:
        if event["event"] == "connect":
            dataQueueConnect.put(event)

def getCleanVal(val,tsClean):
    newVal=val+1
    while newVal in tsClean:
        newVal=val+1
    return newVal

def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        try:
            # convert decimal degrees to radians
            lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
            # haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
            c = 2 * np.arcsin(np.sqrt(a))
            km = 6367 * c
            return km
        except:
            print(lon1, lat1, lon2, lat2)
            traceback.print_exc()

def getUniqueSignalInEvents(eventList):
    signalMapCountries={}
    masterProbeLocList=[]
    seenProbes=set()
    probeIDFilterByDistance={}
    for event in eventList:
        try:
            probeID=int(event['prb_id'])
            if 'All' not in signalMapCountries.keys():
                signalMapCountries['All']=set()
            signalMapCountries['All'].add(probeID)
            if SPLIT_SIGNAL:
                try:
                    country=probeInfo.probeIDToCountryDict[probeID]
                    if country not in signalMapCountries.keys():
                        signalMapCountries[country]=set()
                    signalMapCountries[country].add(probeID)
                except:
                    #No country code available
                    pass
                try:
                    asn=int(probeInfo.probeIDToASNDict[probeID])
                    if asn not in signalMapCountries.keys():
                        signalMapCountries[asn]=set()
                    signalMapCountries[asn].add(probeID)
                except:
                    #No ASN available
                    pass
                try:
                    locDict=probeInfo.probeIDToLocDict[probeID]
                    if probeID not in seenProbes:
                        masterProbeLocList.append([probeID,locDict['lat'],locDict['lon']])
                        seenProbes.add(probeID)
                except:
                    traceback.print_exc()
        except:
            pass # Not a valid event
    if SPLIT_SIGNAL:
        for iter in range(0,len(masterProbeLocList)-1):
            id,lat,lon=masterProbeLocList[iter]
            for iter2 in range(iter+1,len(masterProbeLocList)):
                id2,lat2,lon2=masterProbeLocList[iter2]
                dist=haversine(lon,lat,lon2,lat2)
                if dist<=probeClusterDistanceThreshold:
                    prKey='pid-'+str(id)
                    if prKey not in probeIDFilterByDistance.keys():
                        probeIDFilterByDistance[prKey]=set()
                        probeIDFilterByDistance[prKey].add(id)
                    probeIDFilterByDistance[prKey].add(id2)
        #Add unique sets to main dict
        ignoreID=[]
        for prbID , prbSet in probeIDFilterByDistance.items():
            if prbID in ignoreID:
                continue
            redundantSet=False
            for prbID2 , prbSet2 in probeIDFilterByDistance.items():
                if prbID!=prbID2:
                    if prbSet==prbSet2:
                        ignoreID.append(prbID2)
            if prbID not in signalMapCountries.keys():
                signalMapCountries[prbID]=set()
            signalMapCountries[prbID]=prbSet

    logging.info('Events from {0} probes observed'.format(len(seenProbes)))
    return signalMapCountries

def applyBurstThreshold(burstsDict,eventsList):
    thresholdedEvents=[]
    for event in eventsList:
        insertFlag=False
        for state,timeDictList in burstsDict.items():
            if state >= BURST_THRESHOLD:
                for timeDict in timeDictList:
                    if float(event['timestamp'])>=timeDict['start'] and float(event['timestamp'])<=timeDict['end']:
                        insertFlag=True
        if insertFlag:
            thresholdedEvents.append(event)
    #countEventsInState(burstsDict,eventsList)
    return thresholdedEvents

def countEventsInState(burstsDict,eventsList):
    dataDict={}
    for state,timeDictList in burstsDict.items():
        numEventsInState=0
        for event in eventsList:
            for timeDict in timeDictList:
                if float(event['timestamp'])>=timeDict['start'] and float(event['timestamp'])<=timeDict['end']:
                    insertFlag=True
                    numEventsInState+=1
        #print(state,numEventsInState)
        dataDict[state]=numEventsInState
    plotter.plotDict(dataDict,'figures/numEventsInStates')

def getFilteredEvents(eventLocal):
    interestingEvents=[]
    for event in eventLocal:
        try:
            if event['prb_id'] in selectedProbeIds:
                interestingEvents.append(event)
        except:
            traceback.print_exc()
            logging.error('Error in selecting interesting events')

    return interestingEvents

def groupByController(eventsList):
    controllerDict={}
    for evt in eventsList:
        controller=evt['controller']
        if controller not in controllerDict.keys():
            controllerDict[controller]=1
        else:
            controllerDict[controller]+=1
    return controllerDict

def groupByProbeID(eventsList):
    probeIDDict={}
    for evt in eventsList:
        prbID=evt['prb_id']
        if prbID not in probeIDDict.keys():
            probeIDDict[prbID]=1
        else:
            probeIDDict[prbID]+=1
    return probeIDDict

def groupByASN(eventsList):
    ASNDict={}
    for evt in eventsList:
        if evt['asn']:
            asn=int(evt['asn'])
            insertBool=True
            if asnFilterEnabled:
                if asn not in filterDict['asn']:
                    insertBool=False
            if insertBool:
                if asn not in ASNDict.keys():
                    ASNDict[asn]=set()
                ASNDict[asn].add(evt['prb_id'])

    filteredASNDict={}
    impactVals=[]
    noInfoASNs=[]
    for k,v in ASNDict.items():
        try:
            impactVals.append(float(len(v))/float(len(probeInfo.asnToProbeIDDict[k])))
        except KeyError:
            logging.warning('Key {0} not found'.format(k))
            noInfoASNs.append(k)
            continue
    avgImapct=np.average(impactVals)/3
    #print(noInfoASNs)
    avgImapct=0
    logging.info('Threshold Average Impact is {0}.'.format(avgImapct))

    for k,v in ASNDict.items():
        try:
            if k not in noInfoASNs:
                numProbesASOwns=len(probeInfo.asnToProbeIDDict[k])
                if numProbesASOwns > 5:
                    numProbesInASDisconnected=len(v)
                    asnImpact=float(numProbesInASDisconnected)/float(numProbesASOwns)
                    if asnImpact > 1:
                        #print('Abnormal AS',k,numProbesInASDisconnected,numProbesASOwns)
                        asnImpact=1
                    #print(float(len(v)),float(len(asnToProbeIDDict[k])))
                    #print(asnImpact,avgImapct)
                    if asnImpact >= avgImapct:
                        filteredASNDict[k]=asnImpact
        except KeyError:
            logging.error('Key {0} not found'.format(k))
            print('Key {0} not found'.format(k))
            exit(1)
    return filteredASNDict

def groupByCountry(eventsList):
    probeIDToCountryDict=probeInfo.probeIDToCountryDict
    CountryDict={}
    for evt in eventsList:
        id=evt['prb_id']
        if id in probeIDToCountryDict.keys():
            insertBool=True
            if countryFilterEnabled:
                if probeIDToCountryDict[id] not in filterDict['country_code']:
                    insertBool=False
            if insertBool:
                if probeIDToCountryDict[id] not in CountryDict.keys():
                    CountryDict[probeIDToCountryDict[id]]=1
                else:
                    CountryDict[probeIDToCountryDict[id]]+=1
        else:
            #x=1
            #if evt['event']=='connect':
            logging.warning('No mapping found for probe ID {0}'.format(id))
    return CountryDict

def kleinberg(data,probesInUnit=1,timeRange=8640000,verbose=5):
    ts = np.array(data)
    #print(numEvents)

    #logging.info('Performing Kleinberg burst detection..')
    #bursts = pybursts.kleinberg(ts,s=2, gamma=0.3)
    bursts = pybursts.kleinberg(ts,s=s, T=timeRange,n=probesInUnit*nScalar,gamma=gamma)
    '''
    # Give dates of prominent bursts
    if verbose is not None:
        for q, ts, te in bursts:
            if q >= verbose:
                print "Burst level %s from %s to %s." % (q, dt.datetime.utcfromtimestamp(ts),
                        dt.datetime.utcfromtimestamp(te))
    '''
    return bursts


def getData(dataFile):
    '''
    try:
        for line in open(dataFile):
            event = ast.literal_eval(line)
            #print(event)
            if event["event"] == "disconnect":
                dataQueueDisconnect.put(event)
            elif event["event"] == "connect":
                dataQueueConnect.put(event)
    except:
        traceback.print_exc()
    '''
    del dataList[:]
    try:
        data = json.load(gzip.open(dataFile))
        for event in data:
            try:
                dataList.append(event)
                if DETECT_DISCO_BURST:
                    if event["event"] == "disconnect":
                        dataQueueDisconnect.put(event)
                if DETECT_CON_BURST:
                    if event["event"] == "connect":
                        dataQueueConnect.put(event)
            except KeyError:
                pass
    except:
        traceback.print_exc()

def copyToServerFunc(typeFile):
    if copyToServer:
        try:
            command=('sh scpResultsFile.sh {0} {1}'.format(typeFile,plotter.suffix))
            #print(command)
            os.system(command)
        except:
            traceback.print_exc()


def getBurstEventIDDict(burstDict):
    burstEventDict={}
    burstEventID=1
    for state,timeDictList in burstDict.items():
        if state == BURST_THRESHOLD:
            for timeDict in timeDictList:
                burstEventDict[burstEventID]={'start':timeDict['start'],'end':timeDict['end']}
                burstEventID+=1
    return burstEventDict

def getEventID(burstEventDict,event):
    eventID=None
    for eID,times in burstEventDict.items():
        if float(event['timestamp'])>=times['start'] and float(event['timestamp'])<=times['end']:
            eventID=eID
            break
    return eventID

def getTimeStampsForBurstyProbes(burstyProbes,burstDict,burstEventDict):
    burstyProbeInfoDict={}
    for event in dataList:
        if event["event"] == "disconnect":
            eventTime=float(event['timestamp'])
            pid=event["prb_id"]
            if pid in burstyProbes:
                for state,timeDictList in burstDict.items():
                    if state >= BURST_THRESHOLD:
                        eventID=getEventID(burstEventDict,event)
                        for timeDict in timeDictList:
                            if eventID and eventTime>=timeDict['start'] and eventTime<=timeDict['end']:
                                if pid not in burstyProbeInfoDict.keys():
                                    burstyProbeInfoDict[pid]={}
                                if state not in burstyProbeInfoDict[pid].keys():
                                    burstyProbeInfoDict[pid][state]={}
                                if eventID not in burstyProbeInfoDict[pid][state].keys():
                                    burstyProbeInfoDict[pid][state][eventID]=[]
                                burstyProbeInfoDict[pid][state][eventID].append(event["timestamp"])

    #pp.pprint(burstyProbeInfoDict)
    return burstyProbeInfoDict

def correlateWithConnectionEvents(burstyProbeInfoDictIn):
    #pp.pprint(burstyProbeInfoDict)
    burstyProbeInfoDict=burstyProbeInfoDictIn
    burstyProbeDurations={}
    for event in dataList:
        if event["event"] == "connect":
            pid=event["prb_id"]
            if pid in burstyProbeInfoDict.keys():
                for state in burstyProbeInfoDict[pid].keys():
                    for burstID,tmpSList in burstyProbeInfoDict[pid][state].items():
                        for tmpS in tmpSList:
                            eventTS=float(event["timestamp"])
                            if eventTS >tmpS:
                                burstyProbeInfoDict[pid][state][burstID].remove(tmpS)
                                duration=eventTS-tmpS
                                if burstID not in burstyProbeDurations.keys():
                                    burstyProbeDurations[burstID]={}
                                if pid not in burstyProbeDurations[burstID].keys():
                                    burstyProbeDurations[burstID][pid]={}
                                if state not in burstyProbeDurations[burstID][pid].keys():
                                    burstyProbeDurations[burstID][pid][state]=[]
                                burstyProbeDurations[burstID][pid][state].append({"disconnect":tmpS,"connect":eventTS,"duration":duration})

    return burstyProbeDurations

def getPerEventStats(burstyProbeDurations,numProbesInUnit,output):
    burstEventInfo=[]
    for id,inDict in burstyProbeDurations.items():
        startTimes=[]
        endTimes=[]
        durations=[]
        probeIds=[]
        for pid,inDict2 in inDict.items():
            maxState=max(inDict2.keys())
            for infoDict in inDict2[maxState]:
                startTimes.append(infoDict["disconnect"])
                endTimes.append(infoDict["connect"])
                durations.append(infoDict["duration"])
                probeIds.append({'probeID':pid,'state':maxState,"start":infoDict["disconnect"],"end":infoDict["connect"]})
        startMedian=np.median(np.array(startTimes))
        endMedian=np.median(np.array(endTimes))
        durationMedian=np.median(np.array(durations))
        burstEventInfo.append([id,startMedian,endMedian,durationMedian,numProbesInUnit,probeIds])
        output.write([id,startMedian,endMedian,durationMedian,numProbesInUnit,probeIds])
    return burstEventInfo

def workerThread(threadType):
    intConCountryDict={}
    intConControllerDict={}
    intConASNDict={}
    intConProbeIDDict={}
    global numSelectedProbesInUnit #Probes after user filter
    global READ_OK
    numProbesInUnit=0

    while True:
        eventLocal=[]
        if not READ_OK:
            while not READ_OK:
                time.sleep(WAIT_TIME)
        else:
            time.sleep(WAIT_TIME)
        if threadType=='con':
            itemsToRead=dataQueueConnect.qsize()
        elif threadType=='dis':
            itemsToRead=dataQueueDisconnect.qsize()
        else:
            print('Unknown thread type!')
            exit(1)
        itr2=itemsToRead
        if itemsToRead>1:
            while itemsToRead:
                if threadType=='con':
                    event=dataQueueConnect.get()
                else:
                    event=dataQueueDisconnect.get()
                eventLocal.append(event)
                itemsToRead-=1

            interestingEvents=getFilteredEvents(eventLocal)
            dataDate=datetime.fromtimestamp(interestingEvents[0]["timestamp"]).strftime('%Y%m%d')
            signalMapCountries=getUniqueSignalInEvents(interestingEvents)

            #Manage duplicate values
            for key,probeIDSet in signalMapCountries.items():
                numProbesInUnit=0
                asnKey=False
                countryKey=False
                probeKey=False
                allKey=False

                if not SPLIT_SIGNAL:
                    if key !='All':
                        continue

                try:
                    asn=int(key)
                    numProbesInUnit=len(probeInfo.asnToProbeIDDict[asn])
                    asnKey=True
                except:
                    try:
                        if key=='All':
                            numProbesInUnit=numSelectedProbesInUnit
                            allKey=True
                        else:
                            if 'pid' in key:
                                numProbesInUnit=len(probeIDSet)
                                probeKey=True
                            else:
                                numProbesInUnit=len(probeInfo.countryToProbeIDDict[key])
                                countryKey=True
                    except:
                        logging.error('Error in getting number of probes in unit for key: {0}'.format(key))
                        print('Error in getting number of probes in unit for key: {0}'.format(key))
                        continue


                if numProbesInUnit < MIN_PROBES:
                    continue

                timestampDict={}
                eventClean=[]
                tsClean=[]
                probesInFilteredData=set()
                for eventVal in interestingEvents:
                    pID=int(eventVal['prb_id'])
                    asn=None
                    try:
                        asn=int(eventVal['asn'])
                    except:
                        pass
                    if (asnKey and key==asn) or (countryKey and key==probeInfo.probeIDToCountryDict[pID]) or (probeKey and int(key.split('-')[1]) in probeIDSet) or allKey:
                        if pID in probeIDSet:
                            tStamp=float(eventVal['timestamp'])
                            eventVal['timestamp']=tStamp
                            if tStamp not in timestampDict.keys():
                                timestampDict[tStamp]=1
                            else:
                                timestampDict[tStamp]+=1
                            eventClean.append(eventVal)
                            probesInFilteredData.add(pID)


                for tStamp,numOfRep in timestampDict.items():
                    for gr in range(1,numOfRep+1):
                        tsClean.append((tStamp)+(gr/numOfRep))

                if len(tsClean)<SIGNAL_LENGTH:
                    continue

                tsClean.sort()
                #print(tsClean)
                if rawDataPlot:
                    titleInfoText='Total probes matching filter: {0}\nNumber of probes seen in connection events: {1}'.format(numProbesInUnit,len(probesInFilteredData))
                    plotter.plotList(tsClean,'figures/'+threadType+'RawData_'+dataDate+'_'+str(key),titleInfo=titleInfoText)
                bursts = kleinberg(tsClean,timeRange=dataTimeRangeInSeconds,probesInUnit=numProbesInUnit)
                if burstDetectionPlot:
                    plotter.plotBursts(bursts,'figures/'+threadType+'Bursts_'+dataDate+'_'+str(key))

                burstsDict={}
                for brt in bursts:
                    q=brt[0]
                    qstart=brt[1]
                    qend=brt[2]
                    if q not in burstsDict.keys():
                        burstsDict[q]=[]
                    tmpDict={'start':float(qstart),'end':float(qend)}
                    burstsDict[q].append(tmpDict)

                thresholdedEvents=applyBurstThreshold(burstsDict,eventClean)

                if len(thresholdedEvents)>0:
                    if groupByCountryPlot:
                        intConCountryDict=groupByCountry(thresholdedEvents)
                    if groupByControllerPlot:
                        intConControllerDict=groupByController(thresholdedEvents)

                    intConProbeIDDict=groupByProbeID(thresholdedEvents)
                    if threadType=='dis':
                        burstyProbeIDs=intConProbeIDDict.keys()
                        burstEventDict=getBurstEventIDDict(burstsDict)
                        burstyProbeInfoDict=getTimeStampsForBurstyProbes(burstyProbeIDs,burstsDict,burstEventDict)
                        burstyProbeDurations=correlateWithConnectionEvents(burstyProbeInfoDict)
                        output=outputWriter(resultfilename='results/discoEventMedians_'+dataDate+'_'+str(key)+'.txt')
                        burstEventInfo=getPerEventStats(burstyProbeDurations,numProbesInUnit,output)
                        #if processTraceroute:
                        #    #Traceroute Processor
                        #    trProcessor=tracerouteProcessor(burstEventInfo,useStream=False)
                        #    trProcessor.pullTraceroutes()
                    if groupByASNPlot:
                        intConASNDict=groupByASN(thresholdedEvents)
                    if groupByCountryPlot and choroplethPlot and not countryKey:
                        with closing(open('data/ne/choro'+threadType+'Data.txt','w')) as fp:
                            print("CC,DISCON",file=fp)
                            for k,v in intConCountryDict.items():
                                probesOwned=len(probeInfo.countryToProbeIDDict[k])
                                probesInEvent=v
                                #Some probes could be added after the probe enrichment data was grabbed
                                if probesInEvent > probesOwned:
                                    #print(k,v,probesOwned)
                                    normalizedVal=1
                                else:
                                    normalizedVal=float(probesInEvent)/probesOwned
                                print("{0},{1}".format(k,normalizedVal),file=fp)
                            #Hack to make sure data has atleast 2 elements
                            if len(intConCountryDict)==1:
                                if 'MC' not in intConCountryDict.keys():
                                    print("{0},{1}".format('MC',0),file=fp)
                                elif 'VA' not in intConCountryDict.keys():
                                    print("{0},{1}".format('VA',0),file=fp)
                            elif len(intConCountryDict)==0:
                                    print("{0},{1}".format('MC',0),file=fp)
                                    print("{0},{1}".format('VA',0),file=fp)

                    plotter.lock.acquire()
                    try:
                        if groupByCountryPlot and not countryKey:
                            plotter.plotDict(intConCountryDict,'figures/'+threadType+'ByCountry_'+dataDate+'_'+str(key))
                        if groupByASNPlot and not asnKey:
                            plotter.plotDict(intConASNDict,'figures/'+threadType+'ByASN_'+dataDate+'_'+str(key))
                        if groupByControllerPlot:
                            plotter.plotDict(intConControllerDict,'figures/'+threadType+'ByController_'+dataDate+'_'+str(key))
                        if groupByProbeIDPlot:
                            plotter.plotDict(intConProbeIDDict,'figures/'+threadType+'ByProbeID_'+dataDate+'_'+str(key))
                        if groupByCountryPlot and choroplethPlot and not countryKey:
                            #if len(intConCountryDict) > 1:
                            plotChoropleth('data/ne/choro'+threadType+'Data.txt','figures/'+threadType+'ChoroPlot_'+dataDate+'_'+str(key)+'_'+plotter.suffix+'.png',plotter.getFigNum())
                        intConControllerDict.clear()
                        intConProbeIDDict.clear()
                        intConASNDict.clear()
                        intConCountryDict.clear()
                    except:
                        traceback.print_exc()
                    finally:
                        plotter.lock.release()
            copyToServerFunc(threadType)
            for iter in range(0,itr2):
                if threadType=='con':
                    dataQueueConnect.task_done()
                else:
                    dataQueueDisconnect.task_done()

if __name__ == "__main__":

    configfile='conf/discoLive.conf'
    config = configparser.ConfigParser()
    try:
        config.sections()
        config.read(configfile)
    except:
        logging.error('Missing config: ' + configfile)
        exit(1)

    try:
        READ_ONILNE=eval(config['RUN_PARAMS']['readStream'])
        BURST_THRESHOLD=int(config['RUN_PARAMS']['burstLevelThreshold'])
        SIGNAL_LENGTH=int(config['RUN_PARAMS']['minimumSignalLength'])
        MIN_PROBES=int(config['RUN_PARAMS']['minimumProbesInUnit'])
        WAIT_TIME=int(config['RUN_PARAMS']['waitTime'])
        DETECT_DISCO_BURST=eval(config['RUN_PARAMS']['detectDisconnectBurst'])
        DETECT_CON_BURST=eval(config['RUN_PARAMS']['detectConnectBurst'])
        processTraceroute=eval(config['RUN_PARAMS']['processTraceroutes'])
        dataYear=config['RUN_PARAMS']['dataYear']
        logLevel=config['RUN_PARAMS']['logLevel'].upper()
        fastLoadProbeInfo=eval(config['RUN_PARAMS']['fastLoadProbeInfo'])
        SPLIT_SIGNAL=eval(config['FILTERS']['splitSignal'])
        gamma=float(config['KLEINBERG']['gamma'])
        s=float(config['KLEINBERG']['s'])
        nScalar=float(config['KLEINBERG']['nScalar'])

        rawDataPlot=eval(config['PLOT_FILTERS']['rawDataPlot'])
        burstDetectionPlot=eval(config['PLOT_FILTERS']['burstDetectionPlot'])
        groupByCountryPlot=eval(config['PLOT_FILTERS']['groupByCountryPlot'])
        groupByASNPlot=eval(config['PLOT_FILTERS']['groupByASNPlot'])
        groupByControllerPlot=eval(config['PLOT_FILTERS']['groupByControllerPlot'])
        groupByProbeIDPlot=eval(config['PLOT_FILTERS']['groupByProbeIDPlot'])
        choroplethPlot=eval(config['PLOT_FILTERS']['choroplethPlot'])
        copyToServer=eval(config['PLOT_FILTERS']['copyToServer'])
    except:
        print('Incorrect or missing parameter(s) in config file!')
        exit(1)

    logging.basicConfig(filename='logs/{0}.log'.format(os.path.basename(sys.argv[0]).split('.')[0]), level=logLevel,\
                        format='[%(asctime)s] [%(levelname)s] %(message)s',datefmt='%m-%d-%Y %I:%M:%S')

    logging.info('---Disco Live Initialized---')
    logging.info('Using conf file {0}'.format(configfile))

    global dataQueueDisconnect
    global dataQueueConnect
    #For plots
    plotter=plotter()
    #Probe Enrichment Info
    probeInfo=probeEnrichInfo(dataYear=dataYear)
    logging.info('Loading Probe Enrichment Info from {0}'.format(dataYear))
    if fastLoadProbeInfo:
        probeInfo.fastLoadInfo()
    else:
        probeInfo.loadInfoFromFiles()



    #Read filters and prepare a set of valid probe IDs
    filterDict=eval(config['FILTERS']['filterDict'])
    probeClusterDistanceThreshold=int(config['FILTERS']['probeClusterDistanceThreshold'])
    numSelectedProbesInUnit=None
    asnFilterEnabled=False
    countryFilterEnabled=False
    selectedProbeIdsASN=set()
    selectedProbeIdsCountry=set()
    for filterType in filterDict.keys():
        if filterType == 'asn':
            asnFilterEnabled=True
            for val in filterDict[filterType]:
                filterValue=int(val)
                try:
                    for id in probeInfo.asnToProbeIDDict[filterValue]:
                        selectedProbeIdsASN.add(id)
                except KeyError:
                    pass
        elif filterType == 'country_code':
            countryFilterEnabled=True
            for val in filterDict[filterType]:
                filterValue=val
                try:
                    for id in probeInfo.countryToProbeIDDict[filterValue]:
                        selectedProbeIdsCountry.add(id)
                except KeyError:
                    pass
    selectedProbeIds=set()
    if asnFilterEnabled and countryFilterEnabled:
        selectedProbeIds=selectedProbeIdsASN.intersection(selectedProbeIdsCountry)
    elif asnFilterEnabled:
        selectedProbeIds=selectedProbeIdsASN
    elif countryFilterEnabled:
        selectedProbeIds=selectedProbeIdsCountry

    if asnFilterEnabled or countryFilterEnabled:
        logging.info('Filter {0} has {1} probes.'.format(filterDict,len(selectedProbeIds)))
    else:
        logging.info('No filter given, will use all probes')
        selectedProbeIds=set(probeInfo.probeIDToCountryDict.keys())

    numSelectedProbesInUnit=len(selectedProbeIds)
    logging.info('Number of probes selected: {0}'.format(numSelectedProbesInUnit))
    #print('Number of probes selected: {0}'.format(numProbesInUnit))

    dataFile=None
    dataTimeRangeInSeconds=None
    #Variable to control when thread starts reading the data queue
    READ_OK=True

    if not READ_ONILNE:
            try:
                dataFile=sys.argv[1]
                if os.path.isfile(dataFile) or dataFile is None:
                    if '_' not in dataFile:
                        logging.error('Name of data file does not meet requirement. Should contain "_".')
                        exit(1)
                    #print(dataTimeRangeInSeconds)
            except:
                logging.warning('Input parameter error, switching back to reading online stream.')
                plotter.setSuffix('live')
                READ_ONILNE=True

    ts = []
    dataQueueDisconnect=Queue.Queue()
    dataQueueConnect=Queue.Queue()
    dataList=[]

    pp=PrettyPrinter()
    #outputDisc=outputWriter(resultfilename='data/discoResultsDisconnections.txt')
    #outputConn=outputWriter(resultfilename='data/discoResultsConnections.txt')


    #Launch threads
    if DETECT_DISCO_BURST:
        for i in range(0,1):
            t = threading.Thread(target=workerThread,args=('dis',))
            t.daemon = True
            t.start()
    if DETECT_CON_BURST:
        for i in range(0,1):
            t = threading.Thread(target=workerThread,args=('con',))
            t.daemon = True
            t.start()

    if READ_ONILNE:
        if WAIT_TIME < 60:
            logging.info('Thread wait time was too low, updated to 60 seconds.')
            WAIT_TIME=60
        dataTimeRangeInSeconds=int(WAIT_TIME)
        logging.info('Reading Online with wait time {0} seconds.'.format(WAIT_TIME))
        try:

            #Read Stream
            atlas_stream = AtlasStream()
            atlas_stream.connect()

            # Probe's connection status results
            channel = "probe"

            atlas_stream.bind_channel(channel, on_result_response)
            #1409132340
            #1409137200
            #stream_parameters = {"startTime":1409132240,"endTime":1409137200,"speed":5}
            stream_parameters = {"enrichProbes": True}
            atlas_stream.start_stream(stream_type="probestatus", **stream_parameters)

            atlas_stream.timeout()
            dataQueueDisconnect.join()
            dataQueueConnect.join()

            # Shut down everything
            atlas_stream.disconnect()
        except:
            print('Unexpected Event. Quiting.')
            logging.error('Unexpected Event. Quiting.')
            atlas_stream.disconnect()
    else:
        try:
            eventFiles=[]
            if os.path.isdir(dataFile):
                eventFiles = [join(dataFile, f) for f in listdir(dataFile) if isfile(join(dataFile, f))]
            else:
                eventFiles.append(dataFile)

            for file in eventFiles:
                if file.endswith('.gz'):
                    logging.info('Processing {0}'.format(file))
                    dataQueueDisconnect=Queue.Queue()
                    dataQueueConnect=Queue.Queue()
                    plotter.setSuffix(os.path.basename(file).split('_')[0])
                    WAIT_TIME=1
                    try:
                        dataTimeRangeInSeconds=int(eval(sys.argv[2]))*100
                    except:
                        dataTimeRangeInSeconds=8640000
                    #Make sure threads wait till the entire file is read
                    READ_OK=False
                    getData(file)
                    READ_OK=True
                    dataQueueDisconnect.join()
                    dataQueueConnect.join()
                else:
                    logging.info('Ignoring file {0}, its not of correct format.'.format(file))
            '''
            try:
                if processTraceroute:
                    if os.path.isdir('results'):
                        resultFiles = [join('results', f) for f in listdir('results') if isfile(join('results', f))]
                    for fname in resultFiles:
                        bInfo=[]
                        with closing(open(fname,'r')) as fp:
                            for lR in fp:
                                bInfo.append(lR.rstrip('\n').split('|'))
                        #Traceroute Processor
                        trProcessor=tracerouteProcessor(bInfo,useStream=False)
                        trProcessor.pullTraceroutes()
            except:
                traceback.print_exc()
            '''


        except:
            logging.error('Error in reading file.')
            raise Exception('Error in reading file.')

    logging.info('---Disco Live Stopped---')