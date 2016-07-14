from __future__ import division, print_function

import configparser
import csv
import json
import logging
import os.path
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
    print(event)
    if event["event"] == "disconnect":
        dataQueueDisconnect.put(event)
    elif event["event"] == "connect":
        dataQueueConnect.put(event)

def getCleanVal(val,tsClean):
    newVal=val+1
    while newVal in tsClean:
        newVal=val+1
    return newVal

def getInterestingEvents(burstsDict,eventLocal):
    interestingEvents=[]
    #maxState = max(burstsDict.keys())
    for maxState in burstsDict.keys():
        #print(maxState,BURST_THRESHOLD)
        if maxState > BURST_THRESHOLD:
            for timeDict in burstsDict[maxState]:
                for event in eventLocal:
                    if event['timestamp']>=timeDict['start'] and event['timestamp']<=timeDict['end']:
                        interestingEvents.append(event)
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
                        print('Abnormal AS',k,numProbesInASDisconnected,numProbesASOwns)
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
            #print('GRP',evt['timestamp'],evt['controller'],probeIDToCountryDict[id])
            if probeIDToCountryDict[id] not in CountryDict.keys():
                CountryDict[probeIDToCountryDict[id]]=1
            else:
                CountryDict[probeIDToCountryDict[id]]+=1
        else:
            #x=1
            #if evt['event']=='connect':
            logging.warning('No mapping found for probe ID {0}'.format(id))
    return CountryDict

def workerD():
    global intDisControllerDict
    global intDisASNDict
    global intDisCountryDict
    global intDisProbeIDDict
    while True:
        eventLocal=[]
        eventClean=[]
        tsClean=[]
        stateAvgBurstRateDict={}
        time.sleep(WAIT_TIME)

        itemsToRead=dataQueueDisconnect.qsize()
        itr2=itemsToRead
        #print('Dis '+str(itemsToRead))
        if itemsToRead>1:
            while itemsToRead:
                event=dataQueueDisconnect.get()
                eventLocal.append(event)
                itemsToRead-=1
                #dataQueueDisconnect.task_done()

            #Manage duplicate values
            for eventVal in eventLocal:
                newVal=int(str(eventVal['timestamp'])+'00')

                while newVal in tsClean:
                    newVal+=1

                tsClean.append(newVal)
                eventVal['timestamp']=newVal
                eventClean.append(eventVal)

            tsClean.sort()
            plotter.plotList(tsClean,'figures/rawDataDisconnections')
            #print(tsClean)
            bursts = kleinberg(tsClean)
            plotter.plotBursts(bursts,'figures/disconnectionBursts')

            burstsDict={}
            for brt in bursts:
                q=brt[0]
                qstart=brt[1]
                qend=brt[2]
                if q not in burstsDict.keys():
                    burstsDict[q]=[]
                tmpDict={'start':qstart,'end':qend}
                burstsDict[q].append(tmpDict)

            interestingEvents=getInterestingEvents(burstsDict,eventLocal)
            intDisControllerDict=groupByController(interestingEvents)
            intDisProbeIDDict=groupByProbeID(interestingEvents)
            intDisASNDict=groupByASN(interestingEvents)
            intDisCountryDict=groupByCountry(interestingEvents)
            with closing(open('data/ne/choroDisData.txt','w')) as fp:
                print("CC,DISCON",file=fp)
                for k,v in intDisCountryDict.items():
                    normalizedVal=float(v)/len(probeInfo.countryToProbeIDDict[k])
                    #print(v,normalizedVal)
                    print("{0},{1}".format(k,normalizedVal),file=fp)

            for iter in range(0,itr2):
                dataQueueDisconnect.task_done()

def workerC():
    global intConCountryDict
    global intConControllerDict
    global intConASNDict
    global intConProbeIDDict

    while True:
        eventLocal=[]
        eventClean=[]
        tsClean=[]
        stateAvgBurstRateDict={}
        time.sleep(WAIT_TIME)

        itemsToRead=dataQueueConnect.qsize()
        itr2=itemsToRead
        #print('Con '+str(itemsToRead))
        if itemsToRead>1:
            while itemsToRead:
                event=dataQueueConnect.get()
                eventLocal.append(event)
                itemsToRead-=1
                #dataQueueConnect.task_done()

            #Manage duplicate values
            for eventVal in eventLocal:
                newVal=int(str(eventVal['timestamp'])+'00')
                while newVal in tsClean:
                    newVal+=1
                tsClean.append(newVal)
                eventVal['timestamp']=newVal
                eventClean.append(eventVal)

            tsClean.sort()
            plotter.plotList(tsClean,'figures/rawDataConnections')
            bursts = kleinberg(tsClean)
            plotter.plotBursts(bursts,'figures/connectionBursts')

            burstsDict={}
            for brt in bursts:
                q=brt[0]
                qstart=brt[1]
                qend=brt[2]
                if q not in burstsDict.keys():
                    burstsDict[q]=[]
                tmpDict={'start':qstart,'end':qend}
                burstsDict[q].append(tmpDict)

            interestingEvents=getInterestingEvents(burstsDict,eventLocal)
            intConCountryDict=groupByCountry(interestingEvents)
            intConControllerDict=groupByController(interestingEvents)
            intConProbeIDDict=groupByProbeID(interestingEvents)
            intConASNDict=groupByASN(interestingEvents)
            with closing(open('data/ne/choroConData.txt','w')) as fp:
                print("CC,DISCON",file=fp)
                for k,v in intConCountryDict.items():
                    normalizedVal=float(v)/len(probeInfo.countryToProbeIDDict[k])
                    print("{0},{1}".format(k,normalizedVal),file=fp)

            for iter in range(0,itr2):
                dataQueueConnect.task_done()




def kleinberg(data, verbose=5):
    # make timestamps relative to the first one
    ts = np.array(data)

    #print('Performing Kleinberg burst detection..')
    bursts = pybursts.kleinberg(ts,s=2, gamma=0.5)#,g_hat=0.18)
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
    try:
       data = json.load(open(dataFile))
       for event in data:
            try:
                #if event["asn"]==7922 or True:
                if event["event"] == "disconnect":
                    dataQueueDisconnect.put(event)
                elif event["event"] == "connect":
                    dataQueueConnect.put(event)
            except KeyError:
                pass
    except:
        traceback.print_exc()

if __name__ == "__main__":
    logging.basicConfig(filename='logs/{0}.log'.format(os.path.basename(sys.argv[0]).split('.')[0]), level=logging.DEBUG,\
                        format='[%(asctime)s] [%(levelname)s] %(message)s',datefmt='%m-%d-%Y %I:%M:%S')

    logging.info('---Disco Live Initialized---')
    configfile='conf/discoLive.conf'
    logging.info('Using conf file {0}'.format(configfile))
    config = configparser.ConfigParser()
    try:
        config.sections()
        config.read(configfile)
    except:
        logging.error('Missing config: ' + configfile)
        exit(1)

    #For plots
    plotter=plotter()

    #Probe Enrichment Info
    logging.info('Loading Probe Enrichment Info..')
    probeInfo=probeEnrichInfo()
    #probeInfo.loadInfoFromFiles()
    probeInfo.loadAllInfo()

    READ_ONILNE=eval(config['RUN_PARAMS']['readStream'])
    BURST_THRESHOLD=int(config['RUN_PARAMS']['burstLevelThreshold'])
    WAIT_TIME=int(config['RUN_PARAMS']['waitTime'])

    if not READ_ONILNE:
        try:
            dataFile=sys.argv[1]
            if '_' not in dataFile:
                logging.error('Name of data file does not meet requirement. Should contain "_".')
                exit(1)
            plotter.setSuffix(os.path.basename(dataFile).split('_')[0])
        except:
            logging.warning('No input file given, switching back to reading online stream.')
            plotter.setSuffix('live')
            READ_ONILNE=True
            #If given wait time is too small wait at least a minute
            if WAIT_TIME < 60:
                WAIT_TIME=60

    ts = []
    dataQueueDisconnect=Queue.Queue()
    dataQueueConnect=Queue.Queue()

    #outputDisc=outputWriter(resultfilename='data/discoResultsDisconnections.txt')
    #outputConn=outputWriter(resultfilename='data/discoResultsConnections.txt')

    #Launch threads
    for i in range(0,1):
        t = threading.Thread(target=workerD)
        t.daemon = True
        t.start()
    for i in range(0,1):
        t = threading.Thread(target=workerC)
        t.daemon = True
        t.start()

    #pprint=PrettyPrinter()

    #Interesting Events Data
    intDisControllerDict={}
    intConControllerDict={}
    intDisProbeIDDict={}
    intConProbeIDDict={}
    intDisASNDict={}
    intConASNDict={}
    intDisCountryDict={}
    intConCountryDict={}

    if READ_ONILNE:
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
            logging.info('Processing {0}'.format(dataFile))
            getData(dataFile)

            dataQueueDisconnect.join()
            dataQueueConnect.join()

            plotter.plotDict(intDisCountryDict,'figures/disconnectionsByCountry')
            plotter.plotDict(intConCountryDict,'figures/connectionsByCountry')
            plotter.plotDict(intDisASNDict,'figures/disconnectionsByASN')
            plotter.plotDict(intConASNDict,'figures/connectionsByASN')
            plotter.plotDict(intDisControllerDict,'figures/disconnectionsByController')
            plotter.plotDict(intConControllerDict,'figures/connectionsByController')
            plotter.plotDict(intDisProbeIDDict,'figures/disconnectionsByProbeID')
            plotter.plotDict(intConProbeIDDict,'figures/connectionsByProbeID')

            if len(intDisCountryDict) > 1:
                plotChoropleth('data/ne/choroDisData.txt','figures/choroDisPlot_'+plotter.suffix+'.png',plotter.getFigNum())
            if len(intConCountryDict) > 1:
                plotChoropleth('data/ne/choroConData.txt','figures/choroConPlot_'+plotter.suffix+'.png',plotter.getFigNum())

        except:
            logging.error('Error in reading file.')
            raise Exception('Error in reading file.')

    logging.info('---Disco Live Terminated---')