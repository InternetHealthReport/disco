import pybursts
import json
import numpy as np
from matplotlib import pylab as plt
import datetime as dt


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
            ts[i+1] += 1

    bursts =  pybursts.kleinberg(ts, s=2, gamma=0.3, g_hat=3600*10)

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
    plt.savefig("bursts.eps")


if __name__ == "__main__":
    data = getData()
    print "number of disconnections: %s" % len(data)

    bursts = kleinberg(data)
    print bursts

    plotBursts(bursts)
