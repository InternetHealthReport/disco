import pickle
from contextlib import closing
import numpy as np
import json

with closing(open('results/aggResults/topEvents.txt')) as fp:
    for lineR in fp:
        vals=lineR.rstrip('\n').split('|')
        outageID,outageStart,outageEnd,probesStr,aggregationStr,_=vals
        probesSet=eval(probesStr)
        aggregationStr2=aggregationStr.replace(' ',',')
        aggregationList=eval(aggregationStr2)

        aggregationDict={'ases':[],'countries':[]}
        for entry in aggregationList:
            try:
                intEnt=int(entry)
                aggregationDict['ases'].append(intEnt)
            except:
                aggregationDict['countries'].append(entry)
        print(outageID,aggregationDict)