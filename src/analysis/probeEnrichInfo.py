import threading
import traceback
import pickle
import json
from os import listdir, makedirs
from os.path import isfile, join, exists
from datetime import datetime

class probeEnrichInfo():

    def __init__(self,dataYear=datetime.now().strftime('%Y')):
        self.lock = threading.RLock()
        self.dataYear=dataYear
        self.asnToProbeIDDict={}
        self.probeIDToASNDict={}
        self.probeIDToCountryDict={}
        self.countryToProbeIDDict={}
        self.probeIDToLocDict={}

    def loadInfoFromFiles(self):
        self.lock.acquire()
        probeDataPath='data/probeArchiveData/{0}'.format(self.dataYear)
        self.probeInfoFiles = [join(probeDataPath, f) for f in listdir(probeDataPath) if isfile(join(probeDataPath, f))]
        try:
            for pFile in self.probeInfoFiles:
                probesInfo=json.load(open(pFile))
                for probe in probesInfo:
                    #Populate asnToProbeIDDict
                    try:
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
                            #Populate probeIDToLocDict
                            lat=probe['latitude']
                            lon=probe['longitude']
                            if lat is not None and lat != "" and lat != 'None':
                                if lon is not None and lon != "" and lon != 'None':
                                    self.probeIDToLocDict[probe['id']]={'lat':lat,'lon':lon}
                    except KeyError:
                        continue
            if not exists('data/quickReadModuleData/'):
                makedirs('data/quickReadModuleData/')
            pickle.dump(self.asnToProbeIDDict,open('data/quickReadModuleData/'+self.dataYear+'_asnToProbeIDDict.pickle','wb'))
            pickle.dump(self.probeIDToASNDict,open('data/quickReadModuleData/'+self.dataYear+'_probeIDToASNDict.pickle','wb'))
            pickle.dump(self.probeIDToCountryDict,open('data/quickReadModuleData/'+self.dataYear+'_probeIDToCountryDict.pickle','wb'))
            pickle.dump(self.countryToProbeIDDict,open('data/quickReadModuleData/'+self.dataYear+'_countryToProbeIDDict.pickle','wb'))
            pickle.dump(self.probeIDToLocDict,open('data/quickReadModuleData/'+self.dataYear+'_probeIDToLocDict.pickle','wb'))
        except:
            print('Error: Missing probeArchive files ?')
            traceback.print_exc()
        finally:
            self.lock.release()

    def fastLoadInfo(self):
        self.lock.acquire()
        try:
            self.asnToProbeIDDict=pickle.load(open('data/quickReadModuleData/'+self.dataYear+'_asnToProbeIDDict.pickle'))
            self.probeIDToASNDict=pickle.load(open('data/quickReadModuleData/'+self.dataYear+'_probeIDToASNDict.pickle'))
            self.probeIDToCountryDict=pickle.load(open('data/quickReadModuleData/'+self.dataYear+'_probeIDToCountryDict.pickle'))
            self.countryToProbeIDDict=pickle.load(open('data/quickReadModuleData/'+self.dataYear+'_countryToProbeIDDict.pickle'))
            self.probeIDToLocDict=pickle.load(open('data/quickReadModuleData/'+self.dataYear+'_probeIDToLocDict.pickle'))
        except:
            self.loadInfoFromFiles()
        finally:
            self.lock.release()