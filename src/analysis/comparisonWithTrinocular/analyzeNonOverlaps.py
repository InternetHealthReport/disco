from __future__ import division,print_function
from contextlib import closing


import traceback
import numpy as np

outagesToFocus={}
#Read outage that need to be analyzed
with closing(open("results/trinoCompareMaster.csv",'r')) as fp:
    for line in fp:
        vals = line.rstrip('\n').split('|')
        (outID,numPrefixesDiscoDetected,numPrefixesAvailableData,numPrefixesTrinocularDetected,percentage)=vals
        outagesToFocus[int(outID)]={'numPrefixesDiscoDetected':numPrefixesDiscoDetected,\
                                  'numPrefixesAvailableData':numPrefixesAvailableData,\
                                  'numPrefixesTrinocularDetected':numPrefixesTrinocularDetected,\
                                  'percentage':float(percentage)}

#Load add events
eventsMasterDict={}
with closing(open('results/aggResults/topEvents.txt','r')) as fp:
    for lR in fp:
        outageID,outageStartStr,outageEndStr,probeSet,aggregation,burstID=lR.rstrip('\n').split('|')
        eventsMasterDict[int(outageID)]=[float(outageStartStr),float(outageEndStr),eval(probeSet),aggregation,burstID]

with closing(open("data/outageEvalForTMAFinal30min.txt",'r')) as fp:
    trrListForSuccAverage = []
    trrListForFailAverage = []
    durationList=[]
    for line in fp:
        vals=line.rstrip('\n').split('|')
        if vals[0] in ['outageID']:#Add IDs to ignore here
            continue

        # print(len(outageIDsToConsider))
        if int(vals[0]) not in outagesToFocus.keys():
            continue

        (outageID, trRateSucc, trRateOtSucc, trrRefSucc, trrCalcSucc, trRateFail, trRateOtFail, trrRefFail, \
         trrCalcFail, percentageFailedTraceroutes, percentageCouldPredictNextIP, problemProbes, lenProblemProbes, \
         problemProbePrefixes24, lenProblemProbePrefixes24, problemPrefixes, lenProblemPrefixes, problemPrefixes24, \
         lenProblemPrefixes24, defOutagePrefixes, lenDefOutagePrefixes, defOutageProbePrefixes, \
         lenDefOutageProbePrefixes, problemAS, problemProbeAS) = vals


        # Change this filter to look at various outages
        #if (outagesToFocus[int(outageID)]['percentage']==0):
        #if (outagesToFocus[int(outageID)]['percentage']>0 and outagesToFocus[int(outageID)]['percentage']<=50):
        #if (outagesToFocus[int(outageID)]['percentage']>50 and outagesToFocus[int(outageID)]['percentage']<=70):
        #if (outagesToFocus[int(outageID)]['percentage'] > 70 and outagesToFocus[int(outageID)]['percentage'] <= 99):
        if (outagesToFocus[int(outageID)]['percentage']==100):
            duration=float(eventsMasterDict[int(outageID)][1]-eventsMasterDict[int(outageID)][0])/60/60
            print(outageID,duration,trrCalcSucc,trrCalcFail)
            trrListForSuccAverage.append(float(trrCalcSucc))
            trrListForFailAverage.append(float(trrCalcFail))
            durationList.append(duration)

    print(np.average(trrListForSuccAverage),np.average(durationList))