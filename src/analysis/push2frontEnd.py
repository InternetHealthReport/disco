import sys
import json
from collections import defaultdict
from datetime import datetime
import psycopg2
import pymongo
import numpy as np
import smtplib
import emailConf
from email.mime.text import MIMEText

from bson import objectid


def sendMail(message):
    """
    Send an email with the given message.
    The destination/source addresses are defined in emailConf.
    """

    msg = MIMEText(message)
    msg["Subject"] = "RTT analysis stopped on %s (UTC)!" % datetime.utcnow()
    msg["From"] = emailConf.orig 
    msg["To"] = ",".join(emailConf.dest)

    # Send the mail
    server = smtplib.SMTP(emailConf.server)
    server.starttls()
    server.login(emailConf.username, emailConf.password)
    server.sendmail(emailConf.orig, emailConf.dest, msg.as_string())
    server.quit()


def addZeros(cursor, streamName, streamType, discoStream, lastPush, lastAnalysis):

    if streamName in discoStream:
        #fill the holes
        eventList = discoStream[streamName]
        eventList.sort(key=lambda x: x[0])
        ts = lastPush["timestamp"]
        for t1, t2 in eventList:
            cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime, avglevel, nbdiscoprobes, totalprobes) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", (streamType, streamName, datetime.utcfromtimestamp(ts), datetime.utcfromtimestamp(t1), 0, 0, 0))
            ts = t2
        if ts < lastAnalysis["timestamp"]:
            cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime, avglevel, nbdiscoprobes, totalprobes) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", (streamType, streamName, datetime.utcfromtimestamp(ts), datetime.utcfromtimestamp(lastAnalysis["timestamp"]), 0, 0, 0))

    else:
        cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime, avglevel, nbdiscoprobes, totalprobes) \
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", (streamType, streamName, datetime.utcfromtimestamp(lastPush["timestamp"]), datetime.utcfromtimestamp(lastAnalysis["timestamp"]), 0, 0, 0))


if __name__ == "__main__":
    try:
        # Check if there is new results
        client = pymongo.MongoClient("mongodb-iijlab",connect=True)
        db = client.disco
        lastPush = db.frontEnd.find().sort("timestamp", pymongo.DESCENDING).limit(1)[0]
        lastAnalysis = db.streamLastUpdateTime.find().sort("timestamp", pymongo.DESCENDING).limit(1)[0]
        count = defaultdict(int)
        
        if lastPush["timestamp"] != lastAnalysis["timestamp"]:
            print("Time window shifted")
            # get a connection, if a connect cannot be made an exception will be raised here
            conn_string = "host='romain.iijlab.net' dbname='ihr'"
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            # Get list of analyzed ASN and countries
            streamInfo = db.streamInfo.find().sort("year",pymongo.DESCENDING).limit(1)[0]
            analyzedAsn = streamInfo["streamsMonitored"]["ases"] 
            analyzedCountries = streamInfo["streamsMonitored"]["countries"] 
            asNames = defaultdict(str, json.load(open("data/asNames.json")))
            countryNames = defaultdict(str, json.load(open("data/countryNames.json")))

            for asn in analyzedAsn:
                cursor.execute("INSERT INTO ihr_asn (number, name) SELECT %s, %s \
                    WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number= %s);", (asn, asNames["AS"+str(asn)], asn))

            for country in analyzedCountries:
                cursor.execute("INSERT INTO ihr_country (code, name) SELECT %s, %s \
                    WHERE NOT EXISTS ( SELECT code FROM ihr_country WHERE code= %s);", (country, countryNames[country], country))


            # Get latest results from mongo
            events = db.streamResults.find({"start": {"$gte": lastPush["timestamp"]}})

            # Update results on the webserver
            discoStream = defaultdict(list) 
            for event in events:

                if event["duration"] < 900:
                    #ignore events shorter than 15 minutes
                    # print("Ignoring new results: %s" % event)
                    continue
                
                # print("Adding new results: %s" % event)
                count[event["streamType"]]+=1
                # compute the average burst level
                avgLevel = np.mean([p["state"] for p in event["probeInfo"]])

                # push event to the webserver
                startDate = datetime.utcfromtimestamp(event["start"])
                endDate = datetime.utcfromtimestamp(event["end"])
                cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime, avglevel, nbdiscoprobes, totalprobes) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", (event["streamType"], event["streamName"], startDate, endDate, avgLevel, len(event["probeInfo"]), event["numberOfProbesInUnit"]))
                # cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime) VALUES (%s, %s, %s, %s) RETURNING id", (event["streamType"], event["streamName"], startDate, endDate))

                eventId = cursor.fetchone()[0]

                # Add corresponding probes infos
                for probe in event["probeInfo"]:
                    cursor.execute("INSERT INTO ihr_disco_probes (probe_id, event_id, starttime, endtime, level) \
                            VALUES (%s, %s, %s, %s, %s) ", (p["probeID"], eventId, datetime.utcfromtimestamp(p["start"]), datetime.utcfromtimestamp(p["end"]), p["state"] ))

                discoStream[event["streamName"]].append( (event["start"], event["end"]) )
                    
            # Update all analyzed streams
            for asn in analyzedAsn:
                addZeros(cursor, asn, "asn", discoStream, lastPush, lastAnalysis) 

            for country in analyzedCountries:
                addZeros(cursor, country, "country", discoStream, lastPush, lastAnalysis) 

            conn.commit()
            cursor.close()
            conn.close()

            db.frontEnd.insert_one({"timestamp": lastAnalysis["timestamp"]})
            for key, count in count.iteritems():
                print("Added %s %s events" % (count, key) )
            print("Good-bye!")


    except Exception as e: 
        save_note = "Exception dump: %s : %s.\nCommand: %s" % (type(e).__name__, e, sys.argv)
        exception_fp = open("dump_%s.err" % datetime.now(), "w")
        exception_fp.write(save_note) 
        sendMail(save_note)

