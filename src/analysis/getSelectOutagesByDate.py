from contextlib import closing
from datetime import datetime

filterYear='2015'
filterMonth='04' #Note month filter is greater than equal to

with closing(open('results/aggResults/topEvents.txt')) as fp:
    for lineR in fp:
        vals=lineR.rstrip('\n').split('|')
        outageID,outageStart,outageEnd,probesStr,aggregationStr,_=vals
        year, month, day = datetime.utcfromtimestamp(float(outageStart)).strftime("%Y-%m-%d").split('-')
        if year==filterYear:
            if int(month)>=int(filterMonth):
                print(outageID,year,month)