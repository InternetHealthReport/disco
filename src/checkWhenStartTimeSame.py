from mongoClient import mongoClient
from datetime import datetime
import configparser
import time

if __name__=='__main__':
    # Read MongoDB config
    dbname = None
    configfile = 'conf/mongodb.conf'
    config = configparser.ConfigParser()
    try:
        config.sections()
        config.read(configfile)
    except:
        logging.error('Missing config: ' + configfile)
        exit(1)

    try:
        dbname = config['MONGODB']['dbname']
    except:
        print('Error in reading mongodb.conf. Check parameters.')
        exit(1)


    # db["streamResults"].aggregate([ {$group : { '_id': {start:"$start",streamName:'$streamName'} , 'count' : { $sum: 1}}}, {$match : { count : { $gt : 1 } }} ])

    while True:
        currentTS = int((datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds())
        mapStartStreamName = {}
        mongodb = mongoClient(dbname)
        entries = mongodb.db['streamResults'].find()
        for ent in entries:
            try:
                key = str(ent['start'])+'|'+ent['streamName']
            except KeyError:
                continue # Some old records
            if key not in mapStartStreamName:
                mapStartStreamName[key] = 1
            else:
                mapStartStreamName[key] += 1


        for k, v in mapStartStreamName.items():
            if v > 1:
                print('{0}: {1} {2}'.format(currentTS,k,v))

        time.sleep(900)

