from __future__ import division
import pybursts
import numpy as np
import threading
import time
from matplotlib import pylab as plt
import datetime as dt
import Queue
import traceback
from contextlib import closing
import os.path
import csv
from pprint import PrettyPrinter
import json
import ast
import sys
from ripe.atlas.cousteau import AtlasStream

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

def groupByASN(eventsList):
    #Load probe info
    asnToProbeIDDict={}
    probeInfoFiles=['data/probeArchive-201600603.json','data/probeArchive-201600604.json','data/probeArchive-201600605.json','data/probeArchive-201600606.json','data/probeArchive-201600607.json']
    for pFile in probeInfoFiles:
        probesInfo=json.load(open(pFile))
        for probe in probesInfo:
            if probe['prefix_v4']!='null':
                asn=probe['asn_v4']
            elif probe['prefix_v6']!='null':
                asn=probe['asn_v6']
            else:
                continue
            if asn not in asnToProbeIDDict.keys():
                asnToProbeIDDict[asn]=set()
            asnToProbeIDDict[asn].add(probe['id'])

    ASNDict={}
    for evt in eventsList:
        asn=evt['asn']
        if asn not in ASNDict.keys():
            ASNDict[asn]=set()
        ASNDict[asn].add(evt['prb_id'])


    filteredASNDict={}
    impactVals=[]
    noInfoASNs=[]
    for k,v in ASNDict.items():
        try:
            impactVals.append(float(len(v))/float(len(asnToProbeIDDict[k])))
        except KeyError:
            print('Key avg {0} not found'.format(k))
            noInfoASNs.append(k)
            continue
    avgImapct=np.average(impactVals)/2
    #print(noInfoASNs)
    print('Average Impact is {0}.'.format(avgImapct))

    for k,v in ASNDict.items():
        try:
            if k not in noInfoASNs:
                numProbesASOwns=len(asnToProbeIDDict[k])
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
            print('Key {0} not found'.format(k))
            exit(1)
    return filteredASNDict

def groupByCountry(eventsList):
    #Load probe info
    probesInfo=json.load(open('data/probeArchive-201600607.json'))
    probeIDToCountryDict={}
    for probe in probesInfo:
        probeIDToCountryDict[probe['id']]=probe['country_code']

    CountryDict={}
    for evt in eventsList:
        id=evt['prb_id']
        if id in probeIDToCountryDict.keys():
            if probeIDToCountryDict[id] not in CountryDict.keys():
                CountryDict[probeIDToCountryDict[id]]=1
            else:
                CountryDict[probeIDToCountryDict[id]]+=1
        else:
            x=1
            #print('No mapping found for probe ID {0}'.format(id))
    return CountryDict

def plotDict(d,outFileName,num):
    try:
        fig = plt.figure(num)
        plt.tick_params(axis='both', which='major', labelsize=7)
        X = np.arange(len(d))
        plt.bar(X, d.values(), align='center')#, width=0.5)
        plt.xticks(X, d.keys(), rotation='45')
        ymax = max(d.values()) + 1
        #plt.ylim(0, ymax)
        #plt.yscale('log')
        #plt.show()
        plt.savefig(outFileName)
    except:
        traceback.print_exc()

def workerD():
    global intDisControllerDict
    global intDisASNDict
    global intDisCountryDict
    while True:
        eventLocal=[]
        eventClean=[]
        tsClean=[]
        stateAvgBurstRateDict={}
        time.sleep(waitTime)

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
            #print(tsClean)
            bursts = kleinberg(tsClean)
            plotBursts(bursts,'discon')

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
            #print(intDisControllerDict)
            intDisASNDict=groupByASN(interestingEvents)
            #pprint.pprint(intDisASNDict)
            intDisCountryDict=groupByCountry(interestingEvents)
            #print(intDisCountryDict)

            for iter in range(0,itr2):
                dataQueueDisconnect.task_done()

def workerC():
    while True:
        eventLocal=[]
        eventClean=[]
        tsClean=[]
        stateAvgBurstRateDict={}
        time.sleep(waitTime)

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
            bursts = kleinberg(tsClean)
            plotBursts(bursts,'con')

            burstsDict={}
            for brt in bursts:
                q=brt[0]
                qstart=brt[1]
                qend=brt[2]
                if q not in burstsDict.keys():
                    burstsDict[q]=[]
                tmpDict={'start':qstart,'end':qend}
                burstsDict[q].append(tmpDict)

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

def plotBursts(bursts,name):
    fig = plt.figure(1)
    #print(bursts)
    b = {}
    for q, ts, te in bursts:
        if not q in b:
            b[q] = {"x":[], "y":[]}

        b[q]["x"].append(dt.datetime.utcfromtimestamp(ts/100))
        b[q]["y"].append(0)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(ts/100))
        b[q]["y"].append(q)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(te/100))
        b[q]["y"].append(q)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(te/100))
        b[q]["y"].append(0)

    for q, val in b.iteritems():
        plt.plot(val["x"], val["y"], label=q)

    plt.ylabel("Burst level")
    fig.autofmt_xdate()
    outfile='figures/bursts_'+name+'.png'
    plt.savefig(outfile)

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
            #print(event)
            if event["event"] == "disconnect":
                dataQueueDisconnect.put(event)
            elif event["event"] == "connect":
                dataQueueConnect.put(event)
    except:
        traceback.print_exc()


if __name__ == "__main__":
    READ_ONILNE=False
    BURST_THRESHOLD=7
    waitTime=1
    try:
        dataFile=sys.argv[1]
    except:
        READ_ONILNE=True
        waitTime=60
        print('Reading Online with wait time 60 seconds..')

    ts = []
    dataQueueDisconnect=Queue.Queue()
    dataQueueConnect=Queue.Queue()
    outputDisc=outputWriter(resultfilename='data/discoResultsDisconnections.txt')
    outputConn=outputWriter(resultfilename='data/discoResultsConnections.txt')

    #Launch threads
    for i in range(0,1):
        t = threading.Thread(target=workerD)
        t.daemon = True
        t.start()
    for i in range(0,1):
        t = threading.Thread(target=workerC)
        t.daemon = True
        t.start()

    pprint=PrettyPrinter()
    #Interesting Events Data

    intDisControllerDict={}
    intDisASNDict={}
    intDisCountryDict={}

    if READ_ONILNE:

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
            atlas_stream.disconnect()
    else:
        try:
            print('Processing {0}'.format(dataFile))
            getData(dataFile)
            dataQueueDisconnect.join()
            dataQueueConnect.join()

            #plotDict(intDisCountryDict,'figures/disconnectionsByCountry.png',2)
            plotDict(intDisASNDict,'figures/disconnectionsByASN.png',3)
            #plotDict(intDisControllerDict,'figures/disconnectionsByController.png',4)


        except:
            raise Exception('Error in reading file.')