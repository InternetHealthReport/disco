from __future__ import print_function
from contextlib import closing
from mongoClient import mongoClient
from ipaddress import IPv4Address,IPv4Network
import sys
import numpy as np

def calcPer(a,b):
    af=float(a)
    bf=float(b)
    if bf==0:
        return 0
    if af>bf:
        return 100
    return (af/bf)*100

# print('Loading trinocular prefixes')
# probedPrefixes=set()
# with closing(open('data/alluniq24sTrinocular.txt','r')) as fp:
#     for lineR in fp:
#         line=lineR.rstrip('\n')
#         try:
#             ip=str(IPv4Address(int(line, 16)))
#             probedPrefixes.add(ip+'/24')
#         except:
#             pass
# print('Finished loading trinocular prefixes')

outageIDToStartTime={}
with closing(open('results/aggResults/topEvents.txt')) as fp:
    for lineR in fp:
        vals=lineR.rstrip('\n').split('|')
        outageID,outageStart,outageEnd,probesStr,aggregationStr,_=vals
        outageIDToStartTime[outageID]=float(outageStart)

mongodb=mongoClient()
allPercentages=[]
#'outageEval/outageEvalForTMAFinal30min.txt'
with closing(open("outageEval/"+sys.argv[1],'r')) as fp:
    for line in fp:
        vals=line.rstrip('\n').split('|')
        if vals[0] in ['outageID']:#Add IDs to ignore here
            continue
        #Now pick on outages from April 2015 to Dec 2015
        #The array was generated using:
        #python3.4 src/analysis/getSelectOutagesByDate.py | awk '{printf($1",")}'
        outageIDsToConsider=[9,10,14,18,19,35,38,40,43,47,49,50,55,60,65,95,112,113,118,128,147,152,154,161,170,\
                                171,179,181,194,203,205,215,221,226,230,238,241,243,244,249,252,260,278,282,286,292,\
                                293,294,298,308,309,311,318,330,334,336,340,353,354,357,358,360,361,365,367,372,383,\
                                388,403,412,417,418,419,422,427,431,435,440,442,443]
        #print(len(outageIDsToConsider))
        if int(vals[0]) not in outageIDsToConsider:
            continue

        #print(vals)
        '''
        (outageID,trRate,percentageFailedTraceroutes,percentageCouldPredictNextIP,\
        problemProbes,lenProblemProbes,problemProbePrefixes24,lenProblemProbePrefixes24,\
        problemPrefixes,lenProblemPrefixes,problemPrefixes24,lenProblemPrefixes24,defOutagePrefixes,\
        lenDefOutagePrefixes,defOutageProbePrefixes,lenDefOutageProbePrefixes,problemAS)=vals
        '''

        (outageID, trRateSucc, trRateOtSucc, trrRefSucc, trrCalcSucc, trRateFail, trRateOtFail, trrRefFail, \
        trrCalcFail, percentageFailedTraceroutes, percentageCouldPredictNextIP, problemProbes, lenProblemProbes,\
        problemProbePrefixes24, lenProblemProbePrefixes24, problemPrefixes, lenProblemPrefixes, problemPrefixes24,\
        lenProblemPrefixes24, defOutagePrefixes, lenDefOutagePrefixes, defOutageProbePrefixes, \
        lenDefOutageProbePrefixes, problemAS, problemProbeAS)=vals

        if lenProblemProbePrefixes24=='NoTR':
            continue

        '''
        #print(outageID,calcPer(lenDefOutageProbePrefixes,lenProblemProbePrefixes24))
        notFoundPrefixes=set() # Contains prefixes that we didnt find
        for prf in eval(problemProbePrefixes24):
            if IPv4Network(prf) not in eval(defOutageProbePrefixes):
                #print(prf,eval(defOutageProbePrefixes))
                notFoundPrefixes.add(prf)

        #print(len(notFoundPrefixes),float(lenProblemProbePrefixes24)-float(lenDefOutageProbePrefixes))



        missedPrefixes=set()
        #for prf in notFoundPrefixes:
        for prf in eval(problemProbePrefixes24):
            #probedBool=mongodb.checkPrefixWasProbed(prf)
            #if probedBool:
            if prf in probedPrefixes:
                missedPrefixes.add(prf)
        lenMissedPrefixes=len(missedPrefixes)
        lenAvailableDataForPrefixes=int(lenDefOutageProbePrefixes)+lenMissedPrefixes

        print(outageID,lenProblemProbePrefixes24,lenAvailableDataForPrefixes,lenDefOutageProbePrefixes,calcPer(lenDefOutageProbePrefixes,lenAvailableDataForPrefixes))
        '''
        #print(problemProbePrefixes24)

        if not int(lenProblemProbePrefixes24) > 0:
            continue

        availableDataFor=set()
        availableDataForM=set()
        for pref in eval(problemProbePrefixes24):
            #if str(pref) in probedPrefixes:
            #    availableDataFor.add(pref)
            if mongodb.checkPrefixWasProbed(pref,outageIDToStartTime[outageID]):
                availableDataForM.add(pref)
        #print(availableDataFor)
        #print(defOutageProbePrefixes)
        #lenAvailableDataForPrefixes=len(availableDataFor)
        lenAvailableDataForPrefixesM = len(availableDataForM)
        #if not lenAvailableDataForPrefixes>0:
        #    continue

        #perc=calcPer(lenDefOutageProbePrefixes, lenAvailableDataForPrefixes)
        percM = calcPer(lenDefOutageProbePrefixes, lenAvailableDataForPrefixesM)
        #allPercentages.append(perc)
        #print(outageID,lenProblemProbePrefixes24,lenAvailableDataForPrefixes,lenDefOutageProbePrefixes,perc)
        infovals=outageID+'|'+str(lenProblemProbePrefixes24)+'|'+str(lenAvailableDataForPrefixesM)+'|'+\
                 str(lenDefOutageProbePrefixes)+'|'+str(percM)
        print(infovals,file=open("trinoCompare/"+outageID+".out",'a'))
#print(np.average(allPercentages))
