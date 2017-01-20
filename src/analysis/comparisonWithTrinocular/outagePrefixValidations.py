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
        if vals[0] in ['outageID']:#Add IDs to ignore here
            continue
        #Now pick on outages from April 2015 to Dec 2015
        #The array was generated using:
        #python3.4 src/analysis/getSelectOutagesByDate.py | awk '{printf($1",")}'
        if int(vals[0]) not in [9,10,14,18,19,35,38,40,43,47,49,50,55,60,65,95,112,113,118,128,147,152,154,161,170,171,\
                                179,181,194,203,205,215,221,226,230,238,241,243,244,249,252,260,278,282,286,292,293,\
                                294,298,308,309,311,318,330,334,336,340,353,354,357,358,360,361,365,367,372,383,388,403,\
                                412,417,418,419,422,427,431,435,440,442,443]

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

        #print(len(notFoundPrefixes),float(lenProblemProbePrefixes24)-float(lenDefOutageProbePrefixes))



        missedPrefixes=set()
        for prf in notFoundPrefixes:
            probedBool=mongodb.checkPrefixWasProbed(prf)
            if probedBool:
                missedPrefixes.add(prf)
        lenMissedPrefixes=len(missedPrefixes)
        lenAvailableDataForPrefixes=int(lenDefOutageProbePrefixes)+lenMissedPrefixes

        print(outageID,lenProblemProbePrefixes24,lenAvailableDataForPrefixes,lenDefOutageProbePrefixes,calcPer(lenDefOutageProbePrefixes,lenAvailableDataForPrefixes))
