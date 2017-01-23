from __future__ import division
from contextlib import closing
import sys
#from plotFunctions import plotter

#plotter=plotter()
lengthOfData=0
dataToPlot=[]
percentages={'0':0,'0to50':0,'50to70':0,'70to99':0,'100':0}

#Data array obtained as: cat trinoCompare/* | sort -n | awk -F'|' '{if($2>1&&$3>1)printf($5",")}'
# where files in trinoCompare were generated using: ls outageEval |  parallel -P 40 "python2.7 src/outagePrefixValidations.py"
#Files in outageEval generated from the manual parallel run of expectedLinkOutageEvaluation.py
#filtered:
data=[100.0,0.0,0.0,100.0,88.8888888889,100.0,100.0,100.0,66.6666666667,100.0,100.0,8.33333333333,94.7368421053,80.0,12.5,100.0,18.1818181818,100.0,0.0,75.0,100.0,0.0,100.0,75.0,7.69230769231,81.25,75.0,40.0,100.0,100.0,100.0,70.0,0.0,90.9090909091,100.0,25.0,26.6666666667,0.0,0.0,100.0,100.0,66.6666666667,100.0,50.0,100.0,100.0,50.0,100.0,93.75,100.0,0.0,100.0,0.0]
#nonfiltered:
#data=[100.0,0.0,0.0,100.0,0.0,88.8888888889,0.0,100.0,0.0,100.0,100.0,100.0,66.6666666667,100.0,100.0,100.0,100.0,8.33333333333,94.7368421053,80.0,12.5,100.0,18.1818181818,0.0,0.0,100.0,0.0,75.0,0.0,100.0,0.0,100.0,75.0,7.69230769231,81.25,75.0,40.0,0.0,0.0,100.0,100.0,100.0,70.0,0.0,90.9090909091,100.0,25.0,26.6666666667,100.0,0.0,0.0,100.0,100.0,66.6666666667,100.0,50.0,100.0,100.0,50.0,100.0,93.75,0.0,100.0,0.0,0.0,100.0,0.0]
lengthOfData=len(data)
for fPerS in data:
    fPer=float(fPerS)
    dataToPlot.append(fPer)
    if (fPer==0):
        percentages['0']+=1
    elif (fPer>0 and fPer<50):
        percentages['0to50']+=1
    elif (fPer>=50 and fPer<70):
        percentages['50to70']+=1
    elif (fPer>=70and fPer<100):
        percentages['70to99']+=1
    elif (fPer==100):
        percentages['100']+=1

print(percentages,lengthOfData)
percs=[]
for k,v in percentages.items():
    perc=round(float(v/lengthOfData)*100,2)
    percs.append(perc)
    print(k,perc)

#plotter.ecdf(dataToPlot,'hitsWithTrinocular.png',xlabel='Percentage of prefixes',ylabel='CDF: #Outages')

#print(sum(percs))