from contextlib import closing
from datetime import datetime
import threading
import os
import traceback
import csv
class outputWriter():
    def __init__(self,resultfilename=None):
        if not resultfilename:
            print('Please give a result filename.')
            exit(0)
        self.lock = threading.RLock()
        self.resultfilename = resultfilename
        if os.path.exists(self.resultfilename):
            os.remove(self.resultfilename)

    def write(self,val,delimiter="|"):
        self.lock.acquire()
        try:
            with closing(open(self.resultfilename, 'a+')) as csvfile:
                writer = csv.writer(csvfile, delimiter=delimiter)
                writer.writerow(val)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

ot=outputWriter(resultfilename='data/outageEvalForTMAFinal30minOnly2015.txt')

filterYear='2015'
filterMonth='04' #Note month filter is greater than equal to

outIDsToSelect=[]
with closing(open('results/trinoCompareMaster.csv')) as fp:
    for lineR in fp:
        vals=lineR.rstrip('\n').split('|')
        outageID,_,_,_,_=vals
        outIDsToSelect.append(outageID)

with closing(open('data/outageEvalForTMAFinal30min.txt')) as fp:
    for lineR in fp:
        vals=lineR.rstrip('\n').split('|')
        (outageID, trRateSucc, trRateOtSucc, trrRefSucc, trrCalcSucc, trRateFail, trRateOtFail, trrRefFail, \
         trrCalcFail, percentageFailedTraceroutes, percentageCouldPredictNextIP, problemProbes, lenProblemProbes, \
         problemProbePrefixes24, lenProblemProbePrefixes24, problemPrefixes, lenProblemPrefixes, problemPrefixes24, \
         lenProblemPrefixes24, defOutagePrefixes, lenDefOutagePrefixes, defOutageProbePrefixes, \
         lenDefOutageProbePrefixes, problemAS, problemProbeAS) = vals

        if outageID in outIDsToSelect:
            ot.write([outageID, trRateSucc, trRateOtSucc, trrRefSucc, trrCalcSucc, trRateFail, trRateOtFail, trrRefFail,
                      trrCalcFail, percentageFailedTraceroutes, percentageCouldPredictNextIP, problemProbes,
                      len(problemProbes), problemProbePrefixes24, \
                      len(problemProbePrefixes24), problemPrefixes, len(problemPrefixes), problemPrefixes24,
                      len(problemPrefixes24), \
                      defOutagePrefixes, len(defOutagePrefixes), defOutageProbePrefixes, len(defOutageProbePrefixes),
                      problemAS, problemProbeAS])

