from plotFunctions import plotter
from probeEnrichInfo import probeEnrichInfo
import operator
from collections import OrderedDict


def groupByCountry():
    probeIDToCountryDict=probeInfo.probeIDToCountryDict
    CountryDict={}
    for id in probeIDToCountryDict.keys():
        country=probeIDToCountryDict[id]
        if  country is not None and country != "":
            if country not in CountryDict.keys():
                CountryDict[country]=1
            else:
                CountryDict[country]+=1
    print(CountryDict)
    return OrderedDict(sorted(CountryDict.items(), key=operator.itemgetter(1),reverse=True))


def groupByController():
    probeIDToCountryDict=probeInfo.probeIDToCountryDict
    CountryDict={}
    for id in probeIDToCountryDict.keys():
        country=probeIDToCountryDict[id]
        if  country is not None and country != "":
            if country not in CountryDict.keys():
                CountryDict[country]=1
            else:
                CountryDict[country]+=1
    print(CountryDict)
    return OrderedDict(sorted(CountryDict.items(), key=operator.itemgetter(1),reverse=True))


if __name__ == "__main__":
    probeInfo=probeEnrichInfo()
    probeInfo.loadAllInfo()
    plotter=plotter()
    plotter.suffix='GT'
    plotter.plotDict(groupByCountry(),'figures/probesInCountry')