from __future__ import division
from contextlib import closing
import sys
from plotFunctions import plotter

plotter=plotter()
plotter.suffix='Both'
lengthOfData=0
dataToPlot=[]
failurePercentages={'NA':0,'0':0,'0to50':0,'50to70':0,'70to90':0,'90to100':0}
with closing(open(sys.argv[1],'r')) as fp:
    for lineR in fp:
        vals=lineR.split('|')
        fPerS=vals[10]
        if 'percentage' in fPerS:
            continue
        lengthOfData+=1
        if fPerS=='NoFTR':
            failurePercentages['NA']+=1
            continue
        fPer=float(fPerS)
        dataToPlot.append(fPer)
        if (fPer==0):
            failurePercentages['0']+=1
        elif (fPer>0 and fPer<50):
            failurePercentages['0to50']+=1
        elif (fPer>=50 and fPer<70):
            failurePercentages['50to70']+=1
        elif (fPer>=70 and fPer<90):
            failurePercentages['70to90']+=1
        elif (fPer>=90):
            failurePercentages['90to100']+=1


percs=[]
for k,v in failurePercentages.items():
    perc=round(float(v/lengthOfData)*100,2)
    percs.append(perc)
    print(k,perc)

plotter.ecdf(dataToPlot,'percentagePredictedTraceroutes',xlabel='Percentage of Traceroutes with Prediction',ylabel='CDF: #Outages')

#print(sum(percs))