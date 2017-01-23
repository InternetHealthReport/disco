import sys
sys.path.append("src")
import glob
import json
import pandas as pd
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
from matplotlib import pylab as plt
import cPickle as pickle

def ecdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, yvals, **kwargs )
    plt.title("$\mu=%.2f, \sigma=%.2f, max=%.2f$" % (np.mean(a), np.std(a), np.max(a)))


def readResults(filesPattern="results/"):
    """
    Read results from files and return them in a data frame
    """

    years = range(2011,2017)
    data = []

    for ye in years:
        for fi in glob.glob(filesPattern+"discoEventMedians_"+str(ye)+"*"):
            # get date and aggregation name from the file name
            filename = fi.rpartition("/")[2].partition(".")[0]
            (_, dt, agg) = filename.split("_")
            for line in open(fi):
                burstID, start, end, dur, aggSize, probes= line.split("|") 

                # look at burst levels
                lvl = []
                probes = json.loads(probes.replace("'", '"'))
                for probe in probes: 
                    lvl.append(probe["state"])

                probes = set([probe["probeID"] for probe in probes])
                nbProbes = len(probes)

                maxlvl = np.max(lvl)
                avglvl = np.mean(lvl)

                data.append( [dt, burstID, float(start), float(end), float(dur), nbProbes, nbProbes/float(aggSize), agg.rstrip("\n"), maxlvl, avglvl, probes] )


    return pd.DataFrame(data, columns=["date", "burstID", "start", "end", "duration", "nbProbes", "probeRatio", "aggregation", "maxBurstLvl", "avgBurstLvl", "probes"] )


def plotthresholdVSnbalarms(data, minThresh=1, maxThresh=22):
    plt.figure()
    for label, duration in [["10 min.", 600], ["30 min.", 1800], ["60 min.", 3600]]:
        y = []
        x = range(minThresh, maxThresh)

        for i in x: 
            grp = topBursts(data, avgBurstLvl=i, duration=duration)
            y.append(len(grp))

        y = np.array(y)/float(y[0])

        plt.plot(x, y, label=label)

        
    plt.xlabel("Threshold value")
    plt.ylabel("Relative nb. of events")
    plt.legend()
    plt.tight_layout()
    plt.savefig("threshold_events.eps")

def plotDistributions(data, durationThreshold=1800):

    longBurst = data[data["duration"]>durationThreshold]
    smallBurst = data[data["duration"]<=durationThreshold]

    plt.figure()
    ecdf(data["duration"])
    plt.xlabel("Burst duration")
    plt.ylabel("CDF")
    plt.xscale("log")
    plt.tight_layout()
    plt.savefig("distribution_duration.eps")

    plt.figure()
    ecdf(data["maxBurstLvl"])
    ecdf(longBurst["maxBurstLvl"])
    plt.xlabel("Max. Burst Level")
    plt.ylabel("CDF")
    plt.tight_layout()
    plt.savefig("distribution_maxBurstLvl.eps")

    plt.figure()
    ecdf(longBurst["avgBurstLvl"], label="Bursts>15min")
    ecdf(smallBurst["avgBurstLvl"], label="Bursts<15min")
    plt.xlabel("Avg. Burst Level")
    plt.ylabel("CDF")
    plt.tight_layout()
    plt.legend(loc=4)
    plt.savefig("distribution_avgBurstLvl.eps")

    plt.figure()
    ecdf(longBurst["probeRatio"], label="Bursts>15min")
    ecdf(smallBurst["probeRatio"], label="Bursts<15min")
    plt.xlabel("Probe Ratio")
    plt.ylabel("CDF")
    plt.tight_layout()
    plt.legend(loc=4)
    plt.savefig("distribution_probeRatio.eps")

    plt.figure()
    plt.plot(data["duration"], data["avgBurstLvl"], "o")
    plt.plot(longBurst["duration"], longBurst["avgBurstLvl"], "o")
    plt.xlabel("Duration")
    plt.ylabel("Avg. Burst Level")
    plt.tight_layout()
    plt.savefig("scatterPlot_duration_avglvl.eps")

    plt.figure()
    plt.plot(data["probeRatio"], data["avgBurstLvl"], "o")
    plt.plot(longBurst["probeRatio"], longBurst["avgBurstLvl"], "o")
    plt.xlabel("Probe Ratio")
    plt.ylabel("Avg. Burst Level")
    plt.tight_layout()
    plt.savefig("scatterPlot_nbProbes_avglvl.eps")


def topBursts(data, avgBurstLvl=12, duration=1800, probeRatio=0.33):
    df = data[(data["avgBurstLvl"]>avgBurstLvl) & (data["duration"]>duration) & (data["probeRatio"]>probeRatio)]
    
    df["astart"] = df["start"]/1800
    df["aend"] = df["end"]/1800
    df["startBin"] = df["astart"].astype(int)
    df["endBin"] = df["aend"].astype(int)

    grp = df.groupby(["startBin", "endBin"])
    # The following would aggregate more bursts:
    # grp = df.groupby(["startBin"])

    outputFile = open("topEvents_thres%s.txt" % avgBurstLvl, "w")
    for i, g in enumerate(grp.groups.keys()):
        event = grp.get_group(g)

        probes = set()
        for p in event["probes"]:
            probes.update(p)

        outputFile.write("%s|%s|%s|%s|%s|%s\n" % (i, event["start"].mean(), event["end"].mean(), probes, list(event["aggregation"].values), list(event["burstID"].values))) 

    return grp


def trinocularAgg(duration=1800 ):
    tri = pickle.load(open("../trinocular/probePrefixOutageTrinocular.pickle","r"))
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

    outputFile = open("trinocularAggregated.txt", "w")
    for i, g in enumerate(grp.groups.keys()):
        event = grp.get_group(g)

        prefixes = set()
        for p in event["prefix"]:
            prefixes.add(p)

        if len(prefixes)>10:
            outputFile.write("%s|%s|%s|%s\n" % (i, event["start"].mean(), event["end"].mean(), prefixes)) 

    return grp

if __name__ == "__main__":
   data = readResults()
   topBursts(data)
   plotDistributions(data)
