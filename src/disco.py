import pybursts
import json
import numpy as np
from matplotlib import pylab as plt
import datetime as dt


def getData(eventType):
    asn = [ 7843, 10796, 11351, 11426, 11427, 12271, 20001]
    # load timestamps
    data = json.load(open("./data/7000_20140827.json"))
    ts = []
    # for line in open("./data/7000_20140827.json"):
        # event = json.loads(line)
    for event in data:
        if event["event"] == eventType and event["asn"] in asn:
            ts.append(event["timestamp"])

    # sort timestamps
    ts.sort()

    return ts

def kleinberg(data, verbose=10):
    # make timestamps relative to the first one
    ts = np.array(data)

    #replace duplicate values
    # TODO implement something nicer
    ts = ts*10
    for i in range(len(ts)-1):
        if ts[i] == ts[i+1]:
            j = 1
            while ts[i+j]==ts[i]:
                ts[i+j] += j
                j+=1

                assert j < 10

    bursts =  pybursts.kleinberg(ts, s=2, gamma=.1, n=1300, T=86400*10)

    # Give dates of prominent bursts
    if verbose is not None:
        for q, ts, te in bursts:
            if q >= verbose:
                print "Burst level %s from %s to %s." % (q, dt.datetime.utcfromtimestamp(ts/10),
                        dt.datetime.utcfromtimestamp(te/10))

    return bursts

def plotBursts(bursts,title):
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
    plt.title(title)
    fig.autofmt_xdate()
    plt.savefig("bursts_%s.eps" % title)


if __name__ == "__main__":
    data_con = getData("connect")
    data_dis = getData("disconnect")
    print "number of disconnections: %s" % len(data_dis)
    bursts = kleinberg(data_dis)
    plotBursts(bursts, "disconnect")

    print "number of connections: %s" % len(data_con)
    bursts = kleinberg(data_con)
    plotBursts(bursts,"connect")


