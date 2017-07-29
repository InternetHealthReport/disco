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
from emailWithAttachment import *
#from tracerouteProcessor import tracerouteProcessor
from mongoClient import mongoClient
from pprint import PrettyPrinter
from datetime import datetime
import gzip
import collections
from os import listdir
from os.path import isfile, join
from datetime import datetime
from ripe.atlas.cousteau import AtlasResultsRequest


class outputWriter():

    def __init__(self,resultfilename=None):
        if not resultfilename:
            print('Please give a result filename.')
            exit(0)
        self.lock = threading.RLock()
        self.resultfilename = resultfilename
        if os.path.exists(self.resultfilename):
            os.remove(self.resultfilename)
        self.dbname=None
        # Read MongoDB config
        configfile = 'conf/mongodb.conf'
        config = configparser.ConfigParser()
        try:
            config.sections()
            config.read(configfile)
        except:
            logging.error('Missing config: ' + configfile)
            exit(1)

        try:
            self.dbname = config['MONGODB']['dbname']
        except:
            print('Error in reading mongodb.conf. Check parameters.')
            exit(1)

        #self.mongodb = mongoClient(DBNAME)

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

    def updateProbeInfo(self,infoDict1,infoDict2):
        # "probeInfo" : [ { "start" : 1500897808, "probeID" : 3264, "end" : 1500900061, "state" : 20 } ] }
        newInfoDict = {}
        for inDict in [infoDict1,infoDict2]:
            for pDict in inDict:
                if pDict['probeID'] not in newInfoDict.keys():
                    if pDict['end'] != -1:
                        newInfoDict[pDict['probeID']] = {'start':pDict['probeID'],'end':pDict['end'],\
                                                         'state':pDict['state'],'probeID':pDict['probeID'],\
                                                         'prefix_v4':pDict['prefix_v4'],'address_v4':pDict['address_v4']}
                else:
                    # Pick min start
                    newInfoDict[pDict['probeID']]['start'] = min(newInfoDict[pDict['probeID']]['start'], pDict['start'])
                    # Pick max end
                    newInfoDict[pDict['probeID']]['end'] = max(newInfoDict[pDict['probeID']]['end'], pDict['end'])
                    # Pick max state
                    newInfoDict[pDict['probeID']]['state'] = max(newInfoDict[pDict['probeID']]['state'], pDict['state'])
        retList = [p for p in newInfoDict.values()]
        return retList

    def toMongoDB(self,val):
        mongodb = mongoClient(self.dbname)
        (id, startMedian, endMedian, durationMedian, numProbesInUnit, probeIds)=val
        #results={}
        streamName=self.resultfilename.split('/')[1].split('.')[0].split('_')[2]#Gives the stream name
        streamType=None
        try:
            asnum=int(streamName)
            streamType='asn'
        except:
            if '-' in streamName:
                streamType = 'geo'
            else:
                streamType = 'country'

        # Keep track of all conf settings
        confParams={'probeInfoDataYear':dataYear,'burstLevelThreshold':BURST_THRESHOLD,\
                    'minimumSignalLength':SIGNAL_LENGTH,'minimumProbesInUnit':MIN_PROBES,\
                    'probeClusterDistanceThreshold':probeClusterDistanceThreshold}
        insertTime = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
        results={'streamName':streamName,'streamType':streamType,'start':startMedian,'end':endMedian,\
                 'duration':durationMedian,'numberOfProbesInUnit':numProbesInUnit,'probeInfo':probeIds, \
                 'confParams':confParams,'insertTime':insertTime}
        collectionName='streamResults'

        incFlag = False
        if results['duration'] == -1:
            incFlag = True
        sys.stdout.flush()
        #return
        # Check if this event has an entry
        entries = mongodb.db[collectionName].find({'streamName':streamName,'duration':-1})
        if entries.count() > 0:
            if not incFlag:
                entry = entries[0]
                print('---------')
                print('Updated '+str(entry["_id"]))
                print(results)
                print('---------')
                #startOfEventRecorded = float(entry['start'])
                #startMinimum = min(startOfEventRecorded, startMedian)
                newProbeInfo = self.updateProbeInfo(entry['probeInfo'], probeIds)
                try:
                    mongodb.db[collectionName].update({"_id": entry["_id"]}, {'$set':{"start":startMedian,"end": endMedian, "duration":durationMedian ,\
                                                                          'probeInfo':newProbeInfo,'insertTime':insertTime}})

                except:
                    traceback.print_exc()
        else:
            # There was no previously ongoing event for this stream, but a complete event overlapping could exists. Merge them. Only when pushing compelete events
            if not incFlag:
                insertFlag = False
                # Case 1: before
                entriesCheckOverlap = mongodb.db[collectionName].find({'streamName': streamName, 'start': {'$gt':startMedian,'$lt':endMedian}, 'end':{'$gt':endMedian}})
                if entriesCheckOverlap.count() > 0:
                    # Update event
                    entry = entriesCheckOverlap[0]
                    newDuration = float(entry['end']) - float(startMedian)
                    # Update probeInfo
                    newProbeInfo = self.updateProbeInfo(entry['probeInfo'],probeIds)
                    mongodb.db[collectionName].update({"_id": entry["_id"]}, {'$set': {"start": startMedian, "duration": newDuration, \
                                 'probeInfo': newProbeInfo, 'insertTime': insertTime}})
                    insertFlag = True

                # Case 2: contained
                entriesCheckOverlap2 = mongodb.db[collectionName].find(
                    {'streamName': streamName, 'start': {'$lt': startMedian},'end': {'$gt': endMedian}})
                if entriesCheckOverlap2.count() > 0:
                    # Update event
                    entry = entriesCheckOverlap2[0]
                    # start, end and duration don't change. Just update probeIDs
                    newProbeInfo = self.updateProbeInfo(entry['probeInfo'], probeIds)
                    mongodb.db[collectionName].update({"_id": entry["_id"]},
                                                      {'$set': {'probeInfo': newProbeInfo, 'insertTime': insertTime}})
                    insertFlag = True

                # Case 3: after
                entriesCheckOverlap3 = mongodb.db[collectionName].find(
                    {'streamName': streamName, 'start': {'$lt': startMedian},'end': {'$lt': endMedian,'$gt':startMedian}})
                if entriesCheckOverlap3.count() > 0:
                    # Update event
                    entry = entriesCheckOverlap3[0]
                    # start, end and duration don't change. Just update probeIDs
                    newProbeInfo = self.updateProbeInfo(entry['probeInfo'], probeIds)
                    newDuration = float(endMedian) - float(entry['start'])
                    mongodb.db[collectionName].update({"_id": entry["_id"]},
                                                      {'$set': {'probeInfo': newProbeInfo, 'insertTime': insertTime, 'end':endMedian,\
                                                                'duration':newDuration}})
                    insertFlag = True

                # Case 4: covering
                entriesCheckOverlap4 = mongodb.db[collectionName].find(
                    {'streamName': streamName, 'start': {'$gt': startMedian},'end': {'$lt': endMedian}})
                if entriesCheckOverlap4.count() > 0:
                    # Update event
                    entry = entriesCheckOverlap4[0]
                    newProbeInfo = self.updateProbeInfo(entry['probeInfo'], probeIds)
                    newDuration = float(endMedian) - float(startMedian)
                    mongodb.db[collectionName].update({"_id": entry["_id"]},
                                                      {'$set': {'probeInfo': newProbeInfo, 'insertTime': insertTime, \
                                                                'start':startMedian,'end':endMedian,\
                                                                'duration':newDuration}})
                    insertFlag = True

                if not insertFlag:
                    # First time insert
                    mongodb.insertLiveResults(collectionName, results)

            else:
                mongodb.insertLiveResults(collectionName,results)
                print('---------')
                entries = mongodb.db[collectionName].find({'start': startMedian, 'streamName': streamName, 'end': -1})
                print('Inserted overlapping outage: ' + str(entries[0]["_id"]))
                print(results)
                print('---------')

    def pushProbeInfoToDB(self,probeInfo):
        mongodb = mongoClient(self.dbname)
        collectionName='streamInfo'
        results=mongodb.findInCollection(collectionName,'year',dataYear)
        if len(results) == 0:
            allASes=probeInfo.asnToProbeIDDict.keys()
            allPIDs=probeInfo.probeIDToASNDict.keys()
            allCountries=probeInfo.countryToProbeIDDict.keys()
            #streamInfoData={'year':dataYear,'streamsMonitored':{'ases':allASes,'countries':allCountries,'probeIDs':allPIDs}}
            simpleASN2PID={}
            for k,v in probeInfo.asnToProbeIDDict.items():
                simpleASN2PID[str(k)]=list(set(v))
            simpleCountry2PID = {}
            for k,v in probeInfo.countryToProbeIDDict.items():
                simpleCountry2PID[k]=list(set(v))
            streamInfoData = {'year': dataYear,
                              'streamsMonitored': {'ases': simpleASN2PID, \
                                                   'countries': simpleCountry2PID}}
            mongodb.insertLiveResults(collectionName, streamInfoData)

    def updateCurrentTimeInDB(self,ts):
        mongodb = mongoClient(self.dbname)
        mongodb.updateLastSeenTime(ts)


"""Methods for atlas stream"""

class ConnectionError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def on_result_response(*args):
    """
    Function that will be called every time we receive a new result.
    Args is a tuple, so you should use args[0] to access the real message.
    """
    # print args[0]
    item = args[0]
    event = eval(str(item))
    #print(event)
    dataList.append(event)
    if DETECT_DISCO_BURST:
        if event["event"] == "disconnect":
            dataQueueDisconnect.put(event)
    if DETECT_CON_BURST:
        if event["event"] == "connect":
            dataQueueConnect.put(event)


def on_error(*args):
    #print "got in on_error"
    #print args
    raise ConnectionError("Error")


def on_connect(*args):
    #print "got in on_connect"
    #print args
    return


def on_reconnect(*args):
    #print "got in on_reconnect"
    #print args
    #raise ConnectionError("Reconnection")
    return


def on_close(*args):
    #print "got in on_close"
    #print args
    raise ConnectionError("Closed")


def on_disconnect(*args):
    #print "got in on_disconnect"
    #print args
    #raise ConnectionError("Disconnection")
    return


def on_connect_error(*args):
    #print "got in on_connect_error"
    #print args
    raise ConnectionError("Connection Error")


def on_atlas_error(*args):
    #print "got in on_atlas_error"
    #print args
    return


def on_atlas_unsubscribe(*args):
    #print "got in on_atlas_unsubscribe"
    #print args
    raise ConnectionError("Unsubscribed")


def getLive(allmsm=[7000]):
    # Start time of this script, we'll try to get it working for 1 hour
    starttime = datetime.now()

    lastTimestamp = 0
    currCollection = None
    lastDownload = None
    lastConnection = None

    while True:
        try:
            lastConnection = datetime.now()
            atlas_stream = AtlasStream()
            atlas_stream.connect()
            # Measurement results
            channel = "atlas_result"
            # Bind function we want to run with every result message received
            atlas_stream.socketIO.on("connect", on_connect)
            atlas_stream.socketIO.on("disconnect", on_disconnect)
            atlas_stream.socketIO.on("reconnect", on_reconnect)
            atlas_stream.socketIO.on("error", on_error)
            atlas_stream.socketIO.on("close", on_close)
            atlas_stream.socketIO.on("connect_error", on_connect_error)
            atlas_stream.socketIO.on("atlas_error", on_atlas_error)
            atlas_stream.socketIO.on("atlas_unsubscribed", on_atlas_unsubscribe)
            # Subscribe to new stream
            atlas_stream.bind_channel(channel, on_result_response)

            for msm in allmsm:
                # stream_parameters = {"type": "traceroute", "buffering":True, "equalsTo":{"af": 4},   "msm": msm}
                stream_parameters = {"buffering": True, "equalsTo": {"af": 4}, "msm": msm}
                atlas_stream.start_stream(stream_type="result", **stream_parameters)

            # Run for an hour
            atlas_stream.timeout(3600)
            # Shut down everything, stream might timeout so we disconnect and will reconnect
            atlas_stream.disconnect()
            # Wait a bit if the connection was made less than a minute ago
            time.sleep(10)

        except ConnectionError as e:
            now = datetime.utcnow()
            # print "%s: %s" % (now, e)
            # print "last download: %s" % lastDownload
            # print "last connection: %s" % lastConnection
            atlas_stream.disconnect()
            time.sleep(60)

        except Exception as e:
            save_note = "Exception dump: %s : %s.\nCommand: %s" % (type(e).__name__, e, sys.argv)
            exception_fp = open("dump_%s.err" % datetime.now(), "w")
            exception_fp.write(save_note)
            sys.exit()


""""""
'''
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
'''

def getLiveRestAPI():
    WINDOW = 600
    global READ_OK
    currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
    while True:
        try:
            kwargs = {
                "msm_id": 7000,
                "start": datetime.utcfromtimestamp(currentTS-WINDOW),
                "stop": datetime.utcfromtimestamp(currentTS),
            }
            is_success, results = AtlasResultsRequest(**kwargs).create()
            READ_OK = False
            if is_success:
                for ent in results:
                    on_result_response(ent)
            READ_OK = True
            time.sleep(WINDOW)
            currentTS += (WINDOW + 1)
        except:
            traceback.print_exc()

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
        sys.stdout.flush()
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
    #del dataList[:]
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
    # Extremely unoptimized way to do this. Need to rewrite this function.

    #pp.pprint(burstyProbeInfoDict)
    burstyProbeInfoDict=burstyProbeInfoDictIn
    allInBurstIDs=[]
    burstyProbeDurations={}
    for event in dataList:
        if event["event"] == "connect":
            pid=event["prb_id"]
            if pid in burstyProbeInfoDict.keys():
                for state in burstyProbeInfoDict[pid].keys():
                    for burstID,tmpSList in burstyProbeInfoDict[pid][state].items():
                        allInBurstIDs.append(burstID)
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
    # Remove cases where only less than half probes connected
    cleanBurstyProbeDurations={}
    ongoingBurstIDs=[]
    for bid in burstyProbeDurations.keys():
        lenProbeConnVal=len(burstyProbeDurations[bid])
        if lenProbeConnVal >= float(len(burstyProbeInfoDict.keys()))/2:
            cleanBurstyProbeDurations[bid]=burstyProbeDurations[bid]

    burstyProbeDurationsOngoing={}
    for pid in burstyProbeInfoDict.keys():
        for state in burstyProbeInfoDict[pid].keys():
            for burstID, tmpSList in burstyProbeInfoDict[pid][state].items():
                if burstID in cleanBurstyProbeDurations.keys():
                    continue
                for tmpS in tmpSList:
                    if burstID not in burstyProbeDurationsOngoing.keys():
                        burstyProbeDurationsOngoing[burstID] = {}
                    if pid not in burstyProbeDurationsOngoing[burstID].keys():
                        burstyProbeDurationsOngoing[burstID][pid] = {}
                    if state not in burstyProbeDurationsOngoing[burstID][pid].keys():
                        burstyProbeDurationsOngoing[burstID][pid][state] = []
                        burstyProbeDurationsOngoing[burstID][pid][state].append(
                        {"disconnect": tmpS, "connect": -1, "duration": -1})

    return burstyProbeDurationsOngoing,cleanBurstyProbeDurations

def getPerEventStats(burstyProbeDurations,burstyProbeDurationsOngoing,numProbesInUnit,output):
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
                probeIds.append({'probeID':pid,'state':maxState,"start":infoDict["disconnect"],\
                                 "end":infoDict["connect"],'prefix_v4':probeInfo.probeIDToPrefixv4Dict[pid], \
                                 'address_v4': probeInfo.probeIDToAddrv4Dict[pid]})
        startMedian=np.median(np.array(startTimes))
        endMedian=np.median(np.array(endTimes))
        durationMedian=np.median(np.array(durations))
        burstEventInfo.append([id,startMedian,endMedian,durationMedian,numProbesInUnit,probeIds])
        output.write([id,startMedian,endMedian,durationMedian,numProbesInUnit,probeIds])
        output.toMongoDB([id, startMedian, endMedian, durationMedian, numProbesInUnit, probeIds])


    for id,inDict in burstyProbeDurationsOngoing.items():
        startTimes=[]
        probeIds=[]
        for pid,inDict2 in inDict.items():
            maxState=max(inDict2.keys())
            for infoDict in inDict2[maxState]:
                startTimes.append(infoDict["disconnect"])
                probeIds.append({'probeID':pid,'state':maxState,"start":infoDict["disconnect"],\
                                 "end":-1,'prefix_v4':probeInfo.probeIDToPrefixv4Dict[pid],\
                                'address_v4':probeInfo.probeIDToAddrv4Dict[pid]})
        startMedian=np.median(np.array(startTimes))
        burstEventInfo.append([id,startMedian,-1,-1,numProbesInUnit,probeIds])
        #output.write([id,startMedian,endMedian,durationMedian,numProbesInUnit,probeIds])
        output.toMongoDB([id, startMedian, -1, -1, numProbesInUnit, probeIds])

    return burstEventInfo

def workerThread(threadType):
    intConCountryDict={}
    intConControllerDict={}
    intConASNDict={}
    intConProbeIDDict={}
    global numSelectedProbesInUnit #Probes after user filter
    global READ_OK
    global dataTimeRangeInSeconds
    numProbesInUnit=0
    pendingEvents=collections.deque(maxlen=20000)

    while True:
        eventLocal=[]
        filesToEmail=[]
        if not READ_OK:
            while not READ_OK:
                time.sleep(WAIT_TIME)
        else:
            time.sleep(WAIT_TIME)
        lastQueuedTimestamp=int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
        if threadType=='con':
            itemsToRead=dataQueueConnect.qsize()
        elif threadType=='dis':
            itemsToRead=dataQueueDisconnect.qsize()
        else:
            print('Unknown thread type!')
            exit(1)
        allPrevTS = set()
        for eves in pendingEvents:
            allPrevTS.add(eves['timestamp'])
            eventLocal.append(eves)
        itrFromThread = itemsToRead
        itr2=itrFromThread + len(eventLocal)
        prevEvs = itr2 - itrFromThread
        try:
            if prevEvs > 0:
                microSecAddFactor = (lastQueuedTimestamp - min(allPrevTS)) * 100
                dataTimeRangeInSeconds += microSecAddFactor
        except:
            traceback.print_exc()
        logging.info('Events Info - Current:{0} Previous:{1} Total:{2}'.format(itemsToRead, prevEvs, itr2))
        if itr2 > 1:
            if itemsToRead>1:
                while itemsToRead:
                    if threadType=='con':
                        event=dataQueueConnect.get()
                    else:
                        event=dataQueueDisconnect.get()
                    eventLocal.append(event)
                    itemsToRead-=1

            interestingEvents=getFilteredEvents(eventLocal)
            if len(interestingEvents)<1:
                for iter in range(0, itrFromThread):
                    if threadType=='con':
                        dataQueueConnect.task_done()
                    else:
                        dataQueueDisconnect.task_done()
                continue
            dataDate=datetime.utcfromtimestamp(interestingEvents[0]["timestamp"]).strftime('%Y%m%d')
            (plotter.year,plotter.month,plotter.day) = datetime.utcfromtimestamp(interestingEvents[0]["timestamp"]).strftime('%Y-%m-%d').split('-')
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
                    for gr in range(0,numOfRep):
                        tsClean.append((tStamp)+(gr/numOfRep))

                if len(tsClean)<SIGNAL_LENGTH:
                    continue

                tsClean.sort()

                #print(tsClean)
                if rawDataPlot:
                    titleInfoText='Total probes matching filter: {0}\nNumber of probes seen in connection events: {1}'.format(numProbesInUnit,len(probesInFilteredData))
                    plotter.plotList(tsClean,'figures/'+threadType+'RawData_'+dataDate+'_'+str(key),titleInfo=titleInfoText)
                balancedNumProbes = int(numProbesInUnit * (dataTimeRangeInSeconds / 8640000))
                if balancedNumProbes == 0:
                    balancedNumProbes = 1
                bursts = kleinberg(tsClean,timeRange=dataTimeRangeInSeconds,probesInUnit=balancedNumProbes)
                if burstDetectionPlot:
                    plotter.plotBursts(bursts,'figures/'+threadType+'Bursts_'+dataDate+'_'+str(key))
                    filesToEmail.append('figures/'+threadType+'Bursts_'+dataDate+'_'+str(key)+'_'+str(plotter.suffix)+'.'+str(plotter.outputFormat))

                ##Temp code
                #if str(key)=='All':
                #    print(bursts)
                    #print(tsClean)

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
                logging.info('Number of thresholded events: '+str(len(thresholdedEvents))+' for key: '+str(key))
                if len(thresholdedEvents)>0:
                    sys.stdout.flush()
                    if groupByCountryPlot:
                        intConCountryDict=groupByCountry(thresholdedEvents)
                    if groupByControllerPlot:
                        intConControllerDict=groupByController(thresholdedEvents)

                    intConProbeIDDict=groupByProbeID(thresholdedEvents)
                    if threadType=='dis':
                        burstyProbeIDs=intConProbeIDDict.keys()
                        burstEventDict=getBurstEventIDDict(burstsDict)
                        burstyProbeInfoDict=getTimeStampsForBurstyProbes(burstyProbeIDs,burstsDict,burstEventDict)
                        burstyProbeDurationsOngoing,burstyProbeDurations=correlateWithConnectionEvents(burstyProbeInfoDict)
                        # Probes that had corresponding connect events
                        probesWhichGotConnected=[]
                        for _, inDict in burstyProbeDurations.items():
                            for pid, _ in inDict.items():
                                probesWhichGotConnected.append(pid)
                        probesWhichDidntConnect = []
                        for everyPr in burstyProbeIDs:
                            if everyPr not in probesWhichGotConnected:
                                probesWhichDidntConnect.append(everyPr)
                        # Calculate new pending events
                        newPendingEvents = []
                        for event in eventClean:
                            try:
                                if event['prb_id'] in probesWhichDidntConnect:
                                    newPendingEvents.append(event)
                            except:
                                traceback.print_exc()
                                logging.error('Error in selecting interesting events')
                        # Clean up earlier events that may have been completed now
                        itrEV = 0
                        tmpdeququq = collections.deque(maxlen=20000)
                        for evets in pendingEvents:
                            if evets['prb_id'] not in probesWhichGotConnected:
                                tmpdeququq.append(evets)
                        pendingEvents = tmpdeququq
                        pendingEvents.extend(newPendingEvents)
                        del tmpdeququq

                        output=outputWriter(resultfilename='results/discoEventMedians_'+dataDate+'_'+str(key)+'.txt')
                        if len(burstyProbeDurations)>0:
                            filesToEmail.append(output)
                        logging.info('Burst was seen, call made to events stats.')
                        burstEventInfo=getPerEventStats(burstyProbeDurations,burstyProbeDurationsOngoing,numProbesInUnit,output)
                        #if processTraceroute:
                        #    #Traceroute Processor
                        #    trProcessor=tracerouteProcessor(burstEventInfo,useStream=False)
                        #    trProcessor.pullTraceroutes()
                    #### For plotting ###
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

                    try:
                        send_mail(filesToEmail)
                    except:
                        pass #Will not work outside netsec, ignore

            copyToServerFunc(threadType)
            for iter in range(0,itrFromThread):
                try:
                    #print('Task Done: {0} {1}'.format(iter,itrFromThread))
                    sys.stdout.flush()
                    if threadType=='con':
                        dataQueueConnect.task_done()
                    else:
                        dataQueueDisconnect.task_done()
                except ValueError:
                    pass
        else:
            #logging.info('Events seen: {0}{1}{2}'.format(itemsToRead,itrFromThread,itr2))
            for iter in range(0, itrFromThread):
                try:
                    if threadType == 'con':
                        eve = dataQueueConnect.get()
                        dataQueueConnect.task_done()
                    else:
                        eve = dataQueueDisconnect.get()
                        dataQueueDisconnect.task_done()
                except ValueError:
                    pass
        outputTS = outputWriter(resultfilename='timeupdate.txt')
        outputTS.updateCurrentTimeInDB(lastQueuedTimestamp)
        del outputTS

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
    # Push probe info to DB
    outputObj = outputWriter(resultfilename='results/pInfoOut.txt')
    outputObj.pushProbeInfoToDB(probeInfo)
    del outputObj

    if SIGNAL_LENGTH < 2:
        logging.warning('User given signal length too low, using minimum signal length 2.')
        SIGNAL_LENGTH = 2  # Minimum 2 to detect burst

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
        elif filterType == 'pid':
            countryFilterEnabled=True
            for val in filterDict[filterType]:
                pid1=int(val)
                try:
                    probelDict=probeInfo.probeIDToLocDict[pid1]
                    lat = probelDict['lat']
                    lon = probelDict['lon']
                    for prD,probelDictIn in probeInfo.probeIDToLocDict.items():
                        lat2 = probelDictIn['lat']
                        lon2 = probelDictIn['lon']
                        dist = haversine(lon, lat, lon2, lat2)
                        if dist <= probeClusterDistanceThreshold:
                            selectedProbeIdsCountry.add(prD)
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
    #dataList=[]
    dataList=collections.deque(maxlen=20000)

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
        dataTimeRangeInSeconds=int(WAIT_TIME)*100
        logging.info('Reading Online with wait time {0} seconds.'.format(WAIT_TIME))
        '''
        try:
            
            #Read Stream
            atlas_stream = AtlasStream()
            atlas_stream.connect()

            # Probe's connection status results
            #channel = "atlas_probestatus"
            channel = "atlas_result"
            stream_parameters = {"msm": 7000}

            atlas_stream.bind_channel(channel, on_result_response)
            #1409132340
            #1409137200
            #stream_parameters = {"startTime":1409132240,"endTime":1409137200,"speed":5}
            #stream_parameters = {"enrichProbes": True}
            #atlas_stream.start_stream(stream_type="probestatus", **stream_parameters)
            atlas_stream.start_stream(stream_type="result", **stream_parameters)

            atlas_stream.timeout()
            dataQueueDisconnect.join()
            dataQueueConnect.join()

            # Shut down everything
            atlas_stream.disconnect()
            
        except:
            print('Unexpected Event. Quiting.')
            logging.error('Unexpected Event. Quiting.')
            atlas_stream.disconnect()
        '''
        #getLive()
        getLiveRestAPI()
        dataQueueDisconnect.join()
        dataQueueConnect.join()
    else:
        try:
            eventFiles=[]
            if os.path.isdir(dataFile):
                #eventFiles = [join(dataFile, f) for f in listdir(dataFile) if isfile(join(dataFile, f))]
                for dp, dn, files in os.walk(dataFile):
                    for name in files:
                        eventFiles.append(os.path.join(dp, name))
            else:
                eventFiles.append(dataFile)
            eventFiles = sorted(eventFiles)
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
                    logging.info('Waiting for threads to finishing processing events.')
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