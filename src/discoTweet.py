import twitter
import configparser
from mongoClient import mongoClient

def get_outages(previous_time,current_time):
    # Read MongoDB config
    configfile = 'conf/mongodb.conf'
    config = configparser.ConfigParser()
    try:
        config.sections()
        config.read(configfile)
    except:
        print('Missing config: ' + configfile)
        exit(1)

    try:
        dbname = config['MONGODB']['dbname']
    except:
        print('Error in reading mongodb.conf. Check parameters.')
        exit(1)

    mongodb = mongoClient(dbname)

    entries = mongodb.db['streamResults'].find({'insertTime': {'$gte':previous_time,'$lte':current_time},\
                                                'duration': {'$gte':300}})

    for e in entries:
        print(e)

if __name__=='__main__':
    configfile = 'conf/twitter_auth.conf'
    config = configparser.ConfigParser()
    try:
        config.sections()
        config.read(configfile)
    except:
        print('Missing config: ' + configfile)
        exit(1)

    try:
        consumer_key = config['TWITTER']['consumer_key']
        consumer_secret = config['TWITTER']['consumer_secret']
        access_token_key = config['TWITTER']['access_token_key']
        access_token_secret = config['TWITTER']['access_token_secret']
    except:
        print('Error in reading conf.')
        exit(1)


    api = twitter.Api(consumer_key=consumer_key,
                      consumer_secret=consumer_secret,
                      access_token_key=access_token_key,
                      access_token_secret=access_token_secret)

    #status = api.PostUpdate('Hello World! #ihr #disco')
    get_outages(1502333589,1502393589)