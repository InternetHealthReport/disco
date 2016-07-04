import pybursts
import json
import numpy as np
import threading
import time
from matplotlib import pylab as plt
import datetime as dt
import Queue
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
        dataQueue.put(event["timestamp"])

def getCleanVal(val,tsLocal):
    newVal=val+1
    if newval in tsClean:
        newval=getCleanVal(newVal,tsLocal)
    return newval

def worker():
    while True:
        tsLocal=[]
        tsClean=[]
        time.sleep(20)
        #if dataQueue.qsize() > QUEUE_THRESHOLD:
        #    while dataQueue.qsize() > QUEUE_THRESHOLD:
        itemsToRead=dataQueue.qsize()
        while itemsToRead:
            tsLocal.append(dataQueue.get())
            itemsToRead-=1

            dataQueue.task_done()

                tsLocal.sort()
            bursts = kleinberg(tsLocal)
            print(bursts)
            plotBursts(bursts)

def getData():
    # load timestamps
    ts = []
    for line in open("./data/ke.dat"):
        event = json.loads(line)
        if event["event"] == "disconnect":
            ts.append(event["timestamp"])

    # sort timestamps
    ts.sort()

    return ts

def kleinberg(data, verbose=5):
    # make timestamps relative to the first one
    ts = np.array(data)

    #replace duplicate values
    # TODO implement something nicer
    ts = ts*10
    for i in range(len(ts)-1):
        if ts[i] == ts[i+1]:
            ts[i+1] += 0.1

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
        b[q]["x"].append(dt.datetime.utcfromtimestamp(ts/10))
        b[q]["y"].append(0)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(ts/10))
        b[q]["y"].append(q)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(te/10))
        b[q]["y"].append(q)
        b[q]["x"].append(dt.datetime.utcfromtimestamp(te/10))
        b[q]["y"].append(0)

    for q, val in b.iteritems():
        plt.plot(val["x"], val["y"], label=q)

    plt.ylabel("Burst level")
    fig.autofmt_xdate()
    plt.savefig("bursts.png")


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

    #while True:
    #    data = getData()
    #    print "number of disconnections: %s" % len(data)

    #    bursts = kleinberg(data)
    #    print bursts

    #plotBursts(bursts)

    # Shut down everything
    atlas_stream.disconnect()
