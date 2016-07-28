import threading
import traceback
import pickle
import json
from os import listdir
from os.path import isfile, join

class probeEnrichInfo():

    def __init__(self):
        self.lock = threading.RLock()
        probeDataPath='data/probeArchiveData'
        self.probeInfoFiles = [join(probeDataPath, f) for f in listdir(probeDataPath) if isfile(join(probeDataPath, f))]
        #print(self.probeInfoFiles)
        #self.probeInfoFiles=['data/probeArchive-201600603.json','data/probeArchive-201600604.json',\
                             #'data/probeArchive-201600605.json','data/probeArchive-201600606.json','data/probeArchive-201600607.json']
        self.asnToProbeIDDict={}
        self.probeIDToASNDict={}
        self.probeIDToCountryDict={}
        self.countryToProbeIDDict={}

    def loadInfoFromFiles(self):
        self.lock.acquire()
        try:
            for pFile in self.probeInfoFiles:
                probesInfo=json.load(open(pFile))
                for probe in probesInfo:
                    #Populate asnToProbeIDDict
                    if probe['status']!=3:
                        if probe['prefix_v4']!='null':
                            asn=probe['asn_v4']
                        elif probe['prefix_v6']!='null':
                            asn=probe['asn_v6']
                        else:
                            continue
                        if asn is not None and asn != "" and asn != 'None':
                            if asn not in self.asnToProbeIDDict.keys():
                                self.asnToProbeIDDict[asn]=set()
                            self.asnToProbeIDDict[asn].add(probe['id'])
                            self.probeIDToASNDict[probe['id']]=asn
                        #Populate countryToProbeIDDict
                        country=probe['country_code']
                        if country is not None and country != "" and country != 'None':
                            #Populate probeIDToCountryDict
                            self.probeIDToCountryDict[probe['id']]=country
                            if country not in self.countryToProbeIDDict.keys():
                                self.countryToProbeIDDict[country]=set()
                            self.countryToProbeIDDict[country].add(probe['id'])

            pickle.dump(self.asnToProbeIDDict,open('data/quickReadModuleData/asnToProbeIDDict.pickle','wb'))
            pickle.dump(self.probeIDToASNDict,open('data/quickReadModuleData/probeIDToASNDict.pickle','wb'))
            pickle.dump(self.probeIDToCountryDict,open('data/quickReadModuleData/probeIDToCountryDict.pickle','wb'))
            pickle.dump(self.countryToProbeIDDict,open('data/quickReadModuleData/countryToProbeIDDict.pickle','wb'))
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def loadAllInfo(self):
        self.lock.acquire()
        try:
            self.asnToProbeIDDict=pickle.load(open('data/quickReadModuleData/asnToProbeIDDict.pickle'))
            self.probeIDToASNDict=pickle.load(open('data/quickReadModuleData/probeIDToASNDict.pickle'))
            self.probeIDToCountryDict=pickle.load(open('data/quickReadModuleData/probeIDToCountryDict.pickle'))
            self.countryToProbeIDDict=pickle.load(open('data/quickReadModuleData/countryToProbeIDDict.pickle'))
        except:
            traceback.print_exc()
        finally:
            self.lock.release()