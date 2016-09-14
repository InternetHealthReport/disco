import bz2
import sys
import ipaddress
from mongoClient import mongoClient
from contextlib import closing
import cPickle as pickle

if __name__=="__main__":
    filename=sys.argv[1]
    ##Mongo connection
    mongodb=mongoClient()
    f = bz2.BZ2File(filename, 'rb')

    for line in f:
        lineStr=line.rstrip('\n')
        if '#' in lineStr:
            continue

        vals=lineStr.split('\t')
        ip=ipaddress.IPv4Address(int(vals[0], 16))
        outageStart=float(vals[1])
        year,month,day=datetime.utcfromtimestamp(outageStart).strftime("%Y-%m-%d").split('-')
        duration=float(vals[2])
        outageEnd=outageStart+duration
        status=vals[4]

        californiaBool='NA'
        if 'W' in status:
            californiaBool=0
        elif 'w' in status:
            californiaBool=1

        japanBool='NA'
        if 'J' in status:
            japanBool=0
        elif 'j' in status:
            japanBool=1

        coloradoBool='NA'
        if 'C' in status:
            coloradoBool=0
        elif 'c' in status:
            coloradoBool=1

        greeceBool='NA'
        if 'G' in status:
            greeceBool=0
        elif 'g' in status:
            greeceBool=1

        if (californiaBool==1 or coloradoBool==1 or japanBool==1 or greeceBool==1):
            outagePrefix=str(ip)+'/24'
            thisOutage={'outagePrefix':outagePrefix,'outageStart':outageStart,'outageEnd':outageEnd,'duration':duration,'colorado':coloradoBool,\
                        'japan':japanBool,'california':californiaBool,'greece':greeceBool}

            collection='pingoutage_'+year+month+day
            mongodb.insertPingOutage(collection,thisOutage)

