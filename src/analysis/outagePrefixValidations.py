from contextlib import closing
from mongoClient import mongoClient
from ipaddress import IPv4Address,IPv4Network

def calcPer(a,b):
    af=float(a)
    bf=float(b)
    if bf==0:
        return 0
    return (af/bf)*100

mongodb=mongoClient()

with closing(open('outageEvalData/outageEval30WithLen.txt','r')) as fp:
    for line in fp:
        vals=line.rstrip('\n').split('|')
        if vals[0] in ['outageID','43','24','55','64','10','31','38']:
            continue

        outageID,trRate,percentageFailedTraceroutes,percentageCouldPredictNextIP,\
        problemProbes,lenProblemProbes,problemProbePrefixes24,lenProblemProbePrefixes24,\
        problemPrefixes,lenProblemPrefixes,problemPrefixes24,lenProblemPrefixes24,defOutagePrefixes,\
        lenDefOutagePrefixes,defOutageProbePrefixes,lenDefOutageProbePrefixes,problemAS=vals

        if lenProblemProbePrefixes24=='NoTR':
            continue

        #print(outageID,calcPer(lenDefOutageProbePrefixes,lenProblemProbePrefixes24))
        notFoundPrefixes=set() # Contains prefixes that we didnt find
        for prf in eval(problemProbePrefixes24):
            if IPv4Network(prf) not in eval(defOutageProbePrefixes):
                #print(prf,eval(defOutageProbePrefixes))
                notFoundPrefixes.add(prf)

        print(len(notFoundPrefixes),float(lenProblemProbePrefixes24)-float(lenDefOutageProbePrefixes))

        continue

        if outageID=='76':
            print(notFoundPrefixes)

        '''
        missedPrefixes=set()
        for prf in notFoundPrefixes:
            probedBool=mongodb.checkPrefixWasProbed(prf)
            if probedBool:
                missedPrefixes.add(prf)
        lenMissedPrefixes=len(missedPrefixes)
        lenAvailableDataForPrefixes=int(lenDefOutageProbePrefixes)+lenMissedPrefixes

        print(outageID,lenProblemProbePrefixes24,lenAvailableDataForPrefixes,lenDefOutageProbePrefixes,calcPer(lenDefOutageProbePrefixes,lenAvailableDataForPrefixes))
        '''