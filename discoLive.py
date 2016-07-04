from __future__ import division
import pybursts
import numpy as np
import threading
import time
from matplotlib import pylab as plt
import datetime as dt
import Queue
import traceback
from ripe.atlas.cousteau import AtlasStream

def on_result_response(*args):
    """
    Function that will be called every time we receive a new result.
    Args is a tuple, so you should use args[0] to access the real message.
    """
    #print args[0]
    item=args[0]
    event = eval(str(item))
    if event["event"] == "disconnect":
        dataQueue.put(event)

def getCleanVal(val,tsClean):
    newVal=val+1
    while newVal in tsClean:
        newVal=val+1
    return newVal

def worker():
    while True:
        eventLocal=[]
        eventClean=[]
        tsClean=[]
        time.sleep(3*60)
        #if dataQueue.qsize() > QUEUE_THRESHOLD:
            #while dataQueue.qsize() > QUEUE_THRESHOLD:

        itemsToRead=dataQueue.qsize()
        print('Items in queue: {0}'.format(itemsToRead))
        if itemsToRead>0:
            while itemsToRead:
                event=dataQueue.get()
                #eventTimestamp=event['timestamp']
                #eventProbeID=event['id']
                eventLocal.append(event)
                itemsToRead-=1

            #Manage duplicate values
            for eventVal in eventLocal:
                newVal=eventVal['timestamp']
                if newVal in tsClean:
                    newVal=getCleanVal(newVal,tsClean)
                tsClean.append(newVal)
                eventVal['timestamp']=newVal
                eventClean.append(eventVal)

            tsClean.sort()
            bursts = kleinberg(tsClean)
            plotBursts(bursts)
            print('Bursts: {0}'.format(bursts))
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
                    print('----------')
                    print('State {0}:'.format(state))
                    print('Start time: {0}'.format(timesDict['start']))
                    print('End time: {0}'.format(timesDict['end']))
                    print('Probes:')
                    eventCounter=0
                    for evnt in eventClean:
                        if evnt['timestamp']>= timesDict['start'] and evnt['timestamp']<= timesDict['end']:
                            eventCounter+=1
                            print(' Probe ID: {0}'.format(evnt['id']))
                            print(' Prefix: {0}'.format(evnt['prefix']))
                            print(' Country: {0}'.format(evnt['country_code']))
                    burstRate=eventCounter/(timesDict['end']-timesDict['start'])
                    print('Average Burst Rate: {0}'.format(burstRate))
                    print('----------')
                except KeyError:
                    pass
                    #print(evnt)
                except:
                    traceback.print_exc()

            dataQueue.task_done()

def kleinberg(data, verbose=5):
    # make timestamps relative to the first one
    ts = np.array(data)

    print('Performing Kleinberg burst detection..')
    bursts =  pybursts.kleinberg(ts, s=2, gamma=0.3)

    # Give dates of prominent bursts
    if verbose is not None:
        for q, ts, te in bursts:
            if q >= verbose:
                print "Burst level %s from %s to %s." % (q, dt.datetime.utcfromtimestamp(ts/10),
                        dt.datetime.utcfromtimestamp(te/10))

    return bursts

def plotBursts(bursts):
    fig = plt.figure()

    b = {}
    for q, ts, te in bursts:
        if not q in b:
            b[q] = {"x":[], "y":[]}

        # TODO remove /10 when "replace duplicate" is fixed
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
    dataQueue=Queue.Queue()
    for i in range(0,1):
        t = threading.Thread(target=worker)
        t.daemon = True  # Thread dies when main thread (only non-daemon thread) exits.
        t.start()

    #Read Stream
    atlas_stream = AtlasStream()
    atlas_stream.connect()

    # Probe's connection status results
    channel = "probe"
    atlas_stream.bind_channel(channel, on_result_response)
    stream_parameters = {"enrichProbes": True}
    atlas_stream.start_stream(stream_type="probestatus", **stream_parameters)

    atlas_stream.timeout()

    # Shut down everything
    atlas_stream.disconnect()
