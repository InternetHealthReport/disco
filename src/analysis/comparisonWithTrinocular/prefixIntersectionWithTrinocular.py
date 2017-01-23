from __future__ import print_function
from contextlib import closing
import json
import sys
from mongoClient import mongoClient
from multiprocessing import Pool
import ipaddress
'''
Code to check how many prefixes seen by RIPE Atlas are also pinged by Trinocular
'''

def worker(prf):
    mongodb=mongoClient()
    sys.stdout.flush()
    probedBool=mongodb.checkPrefixWasProbed(prf)
    if probedBool:
        with closing(open('probedPrefixesTrinocular.txt','a+')) as fp:
            print(pref,bool,file=fp)


#Get probe prefixes
year='2015'
month='08'
pFile='data/probeArchiveData/'+year+'/probeArchive-'+year+month+'01'+'.json'
probePrefixes24=set()
probesInfo=json.load(open(pFile))
probeIDToCountryDict={}
probeIDToASNDict={}
prefixToProbeIDDict={}
for probe in probesInfo:
    try:
        prIP=probe['address_v4']
        if prIP!='null':
            quads=prIP.split('.')
            prefix24=quads[0]+'.'+quads[1]+'.'+quads[2]+'.'+'0/24'
            probePrefixes24.add(prefix24)
    except:
        continue

#Read the trinocular prefixes
probedPrefixes=set()
with closing(open('data/alluniq24sTrinocular.txt','r')) as fp:
    for lineR in fp:
        line=lineR.rstrip('\n')
        try:
            ip=str(ipaddress.IPv4Address(int(line, 16)))
            probedPrefixes.add(ip+'/24')
        except:
            pass

#p = Pool(50)
#p.map(worker,probePrefixes24)

for prf in probePrefixes24:
    probedBool=0
    if prf in probedPrefixes:
        probedBool=1
    with closing(open('probedPrefixesTrinocular.txt','a+')) as fp:
        print(prf,probedBool,file=fp)

