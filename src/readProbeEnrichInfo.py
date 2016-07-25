from probeEnrichInfo import probeEnrichInfo

probeInfo=probeEnrichInfo()
#probeInfo.loadInfoFromFiles()
probeInfo.loadAllInfo()

for pid,asnVal in probeInfo.probeIDToASNDict.items():
    try:
        asn=int(asnVal)
        print(pid,asn)
    except:
        pass