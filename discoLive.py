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

def workerD():
    while True:
        eventLocal=[]
        eventClean=[]
        tsClean=[]
        stateAvgBurstRateDict={}
        waitTime=(5*60)/5
        #print('----------')
        #print('Thread waiting for {0} seconds...'.format(waitTime))
        time.sleep(waitTime)
        #if dataQueueDisconnect.qsize() > QUEUE_THRESHOLD:
            #while dataQueueDisconnect.qsize() > QUEUE_THRESHOLD:

        itemsToRead=dataQueueDisconnect.qsize()
        if itemsToRead>=1:
            #print('Items in queue: {0}'.format(itemsToRead))
            while itemsToRead:
                event=dataQueueDisconnect.get()
                #eventTimestamp=event['timestamp']
                #eventProbeID=event['id']
                #print(eventProbeID)
                eventLocal.append(event)
                itemsToRead-=1

            #Manage duplicate values
            for eventVal in eventLocal:
                newVal=int(str(eventVal['timestamp'])+'00')
                #print('Detected overlapping timestamps, will flatten data')
                while newVal in tsClean:
                    newVal+=1
                #print('Fixed')
                tsClean.append(newVal)
                eventVal['timestamp']=newVal
                eventClean.append(eventVal)

            tsClean.sort()
            bursts = kleinberg(tsClean)
            #plotBursts(bursts)
            #print('Bursts: {0}'.format(bursts))
            burstsDict={}
            for brt in bursts:
                #print(brt)
                q=brt[0]
                qstart=brt[1]
                qend=brt[2]
                if q not in burstsDict.keys():
                    burstsDict[q]={}
                burstsDict[q]['start']=qstart
                burstsDict[q]['end']=qend
            for state,timesDict in burstsDict.items():
                try:
                    #print('State {0}:'.format(state))
                    #print('Start time: {0}'.format(timesDict['start']))
                    #print('End time: {0}'.format(timesDict['end']))
                    #print('Probes:')
                    eventCounter=0
                    for evnt in eventClean:
                        if evnt['timestamp']>= timesDict['start'] and evnt['timestamp']<= timesDict['end']:
                            eventCounter+=1
                            #print(' Probe ID: {0}'.format(evnt['id']))
                            #print(' Prefix: {0}'.format(evnt['prefix']))
                            #print(' Country: {0}'.format(evnt['country_code']))
                            #print(' ASN: {0}'.format(evnt['asn']))
                    burstRate='-1'
                    if timesDict['end']!=timesDict['start']:
                        burstRate=eventCounter/(timesDict['end']-timesDict['start'])
                        #print('Average Burst Rate: {0}'.format(burstRate))
                    stateAvgBurstRateDict[state]=burstRate
                except KeyError:
                    pass
                    #print(evnt)
                except:
                    traceback.print_exc()
            maxState=max(stateAvgBurstRateDict.keys(), key=int)
            output.write([maxState,round(float(stateAvgBurstRateDict[maxState]),5)])
            #print('----------')

            dataQueueDisconnect.task_done()

def workerC():
    while True:
        eventLocal=[]
        eventClean=[]
        tsClean=[]
        stateAvgBurstRateDict={}
        waitTime=(5*60)/5
        time.sleep(waitTime)
        #if dataQueueConnect.qsize() > QUEUE_THRESHOLD:
            #while dataQueueConnect.qsize() > QUEUE_THRESHOLD:

        itemsToRead=dataQueueConnect.qsize()
        if itemsToRead>=1:
            #print('Items in queue: {0}'.format(itemsToRead))
            while itemsToRead:
                event=dataQueueConnect.get()
                #eventTimestamp=event['timestamp']
                #eventProbeID=event['id']
                #print(eventProbeID)
                eventLocal.append(event)
                itemsToRead-=1

            #Manage duplicate values
            for eventVal in eventLocal:
                newVal=int(str(eventVal['timestamp'])+'00')
                #print('Detected overlapping timestamps, will flatten data')
                while newVal in tsClean:
                    newVal+=1
                #print('Fixed')
                tsClean.append(newVal)
                eventVal['timestamp']=newVal
                eventClean.append(eventVal)

            tsClean.sort()
            bursts = kleinberg(tsClean)
            #plotBursts(bursts)
            #print('Bursts: {0}'.format(bursts))
            burstsDict={}
            for brt in bursts:
                #print(brt)
                q=brt[0]
                qstart=brt[1]
                qend=brt[2]
                if q not in burstsDict.keys():
                    burstsDict[q]={}
                burstsDict[q]['start']=qstart
                burstsDict[q]['end']=qend
            for state,timesDict in burstsDict.items():
                try:
                    eventCounter=0
                    for evnt in eventClean:
                        if evnt['timestamp']>= timesDict['start'] and evnt['timestamp']<= timesDict['end']:
                            eventCounter+=1
                    burstRate='-1'
                    if timesDict['end']!=timesDict['start']:
                        burstRate=eventCounter/(timesDict['end']-timesDict['start'])
                        #print('Average Burst Rate: {0}'.format(burstRate))
                    stateAvgBurstRateDict[state]=burstRate
                except KeyError:
                    pass
                    #print(evnt)
                except:
                    traceback.print_exc()
            maxState=max(stateAvgBurstRateDict.keys(), key=int)
            output2.write([maxState,round(float(stateAvgBurstRateDict[maxState]),5)])
            #print('----------')

            dataQueueConnect.task_done()


def kleinberg(data, verbose=5):
    # make timestamps relative to the first one
    ts = np.array(data)

    #print('Performing Kleinberg burst detection..')
    bursts = pybursts.kleinberg(ts, g_hat=300,s=2, gamma=2)

    '''
    # Give dates of prominent bursts
    if verbose is not None:
        for q, ts, te in bursts:
            if q >= verbose:
                print "Burst level %s from %s to %s." % (q, dt.datetime.utcfromtimestamp(ts),
                        dt.datetime.utcfromtimestamp(te))
    '''
    return bursts

def plotBursts(bursts):
    fig = plt.figure()

    b = {}
    for q, ts, te in bursts:
        if not q in b:
            b[q] = {"x":[], "y":[]}

        b[q]["x"].append(dt.datetime.utcfromtimestamp(ts))
        b[q]["y"].append(0)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(ts))
        b[q]["y"].append(q)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(te))
        b[q]["y"].append(q)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(te))
        b[q]["y"].append(0)

    for q, val in b.iteritems():
        plt.plot(val["x"], val["y"], label=q)

    plt.ylabel("Burst level")
    fig.autofmt_xdate()
    outfile='bursts.png'
    plt.savefig(outfile)

if __name__ == "__main__":
    QUEUE_THRESHOLD=2
    ts = []
    dataQueueDisconnect=Queue.Queue()
    dataQueueConnect=Queue.Queue()
    output=outputWriter(resultfilename='discoResults.csv')
    output2=outputWriter(resultfilename='discoResults2.csv')
    for i in range(0,1):
        t = threading.Thread(target=workerD)
        t.daemon = True  # Thread dies when main thread (only non-daemon thread) exits.
        t.start()
    for i in range(0,1):
        t = threading.Thread(target=workerC)
        t.daemon = True  # Thread dies when main thread (only non-daemon thread) exits.
        t.start()

    try:
        #Read Stream
        atlas_stream = AtlasStream()
        atlas_stream.connect()

        # Probe's connection status results
        channel = "probe"

        atlas_stream.bind_channel(channel, on_result_response)
        #1409132340
        #1409137200
        stream_parameters = {"startTime":1409132240,"endTime":1409137200,"speed":5}
        #stream_parameters = {"enrichProbes": True}
        atlas_stream.start_stream(stream_type="probestatus", **stream_parameters)

        atlas_stream.timeout()
        dataQueueDisconnect.join()
        dataQueueConnect.join()
        # Shut down everything
        atlas_stream.disconnect()
    except:
        print('Unexpected Event. Quiting.')
        atlas_stream.disconnect()
