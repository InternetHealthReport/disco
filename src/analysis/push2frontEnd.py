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
import logging
import traceback

from bson import objectid


def sendMail(message):
    """
    Send an email with the given message.
    The destination/source addresses are defined in emailConf.
    """

    msg = MIMEText(message)
    msg["Subject"] = "Disco push2frontEnd stopped on %s (UTC)!" % datetime.utcnow()
    msg["From"] = emailConf.orig 
    msg["To"] = ",".join(emailConf.dest)

    # Send the mail
    server = smtplib.SMTP(emailConf.server)
    server.starttls()
    server.login(emailConf.username, emailConf.password)
    server.sendmail(emailConf.orig, emailConf.dest, msg.as_string())
    server.quit()


def splitOneHourBin(cursor, streamName, streamType, discoStream,  nbTotalProbes, t1, t2):

    # split things in one hour time bins
    r = list(np.arange(t1, t2, 3600))
    if len(r) == 0:
        return

    if t2 - r[-1] > 1800 or len(r)<2:
        r.append(t2)
    else:
        r[-1] = t2
    ts = None
    for te in r:
        if not ts is None:
            cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime, avglevel, nbdiscoprobes, totalprobes, ongoing) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE) RETURNING id", (streamType, streamName, datetime.utcfromtimestamp(ts), datetime.utcfromtimestamp(te), 0, 0, nbTotalProbes))
        ts = te


def addZeros(cursor, streamName, streamType, discoStream, lastPush, lastAnalysis, nbTotalProbes):

    if streamName in discoStream:
        #fill the holes
        eventList = discoStream[streamName]
        eventList.sort(key=lambda x: x[0])
        ts = lastPush["timestamp"]
        for t1, t2 in eventList:
            splitOneHourBin(cursor, streamName, streamType, discoStream, nbTotalProbes, ts, t1)

            ts = t2

        if ts < lastAnalysis["timestamp"]:
            splitOneHourBin(cursor, streamName, streamType, discoStream, nbTotalProbes, ts, lastAnalysis["timestamp"])

    else:
        splitOneHourBin(cursor, streamName, streamType, discoStream, nbTotalProbes, lastPush["timestamp"], lastAnalysis["timestamp"])


if __name__ == "__main__":
    try:
        FORMAT = '%(asctime)s %(processName)s %(message)s'
        logging.basicConfig(format=FORMAT, filename='push2frontEnd.log', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
        logging.info("Started: %s" % sys.argv)

        # Check if there is new results
        client = pymongo.MongoClient("mongodb-iijlab",connect=True)
        db = client.disco
        lastPush = db.frontEnd.find().sort("timestamp", pymongo.DESCENDING).limit(1)[0]
        lastAnalysis = db.streamLastUpdateTime.find().sort("timestamp", pymongo.DESCENDING).limit(1)[0]
        count = defaultdict(int)
        
        if lastPush["timestamp"] != lastAnalysis["timestamp"]:
            logging.info("Time window shifted: %s -> %s" % (lastPush["timestamp"], lastAnalysis["timestamp"]))
            # get a connection, if a connect cannot be made an exception will be raised here
            conn_string = "host='romain.iijlab.net' dbname='ihr'"
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            # Get list of analyzed ASN and countries
            streamInfo = db.streamInfo.find().sort("year",pymongo.DESCENDING).limit(1)[0]
            analyzedAsn = [asn for asn, probes in streamInfo["streamsMonitored"]["ases"].iteritems() if len(probes)>10]
            analyzedCountries = [cc for cc, probes in streamInfo["streamsMonitored"]["countries"].iteritems() if len(probes)>10]
            asNames = defaultdict(str, json.load(open("data/asNames.json")))
            countryNames = defaultdict(str, json.load(open("data/countryNames.json")))

            for asn in analyzedAsn:
                # cursor.execute("INSERT INTO ihr_asn (number, name, disco) VALUES (%s, %s, TRUE) \
                    # ON CONFLICT (number) DO UPDATE SET disco = TRUE;", (asn, asNames["AS"+str(asn)]))
                cursor.execute("""
                do $$
                begin 
                    insert into ihr_asn(number, name, disco, tartiflette) values(%s, %s, TRUE, FALSE);
                  exception when unique_violation then
                    update ihr_asn set disco = TRUE where number = %s;
                end $$;""", (asn, asNames["AS"+str(asn)], asn))
                

            for country in analyzedCountries:
                # cursor.execute("INSERT INTO ihr_country (code, name, disco) VALUES (%s, %s, TRUE) \
                    # ON CONFLICT (code) DO UPDATE SET disco = TRUE;", (country, countryNames[country]))
                cursor.execute("""
                do $$
                begin 
                    insert into ihr_country(code, name, disco, tartiflette) values(%s, %s, TRUE, FALSE);
                  exception when unique_violation then
                    update ihr_country set disco = TRUE where code = %s;
                end $$;""", (country, countryNames[country], country))
                

            # Update results on the webserver
            # # update ongoing events
            cursor.execute("SELECT streamtype, streamname, starttime FROM ihr_disco_events WHERE ongoing=TRUE")
            ongoingEvents = cursor.fetchall()

            for streamtype, streamname, starttime in ongoingEvents:
                events = db.streamResults.find({"streamType": streamtype, "streamName": streamname, "start":{"$lte": lastPush["timestamp"]}, "end":{"$gte": lastPush["timestamp"]}})
                
                events = list(events)
                if len(events) == 1:
                    #update website
                    avgLevel = np.mean([p["state"] for p in event["probeInfo"]])
                    cursor.execute("UPDATE ihr_disco_events SET endtime=%s, avglevel=%s, nbdiscoprobes=%s ongoing=FALSE \
                        WHERE streamtype=%s and streamname=%s and ongoing=TRUE", (datetime.utcfromtimestamp(event["end"]), avgLevel, len(event["probeInfo"]), streamtype, streamname))

                elif len(events) == 0:
                    #event still ongoing
                    #TODO use the probeinfo here? (to update avgLevel)
                    cursor.execute("UPDATE ihr_disco_events SET endtime=%s \
                        WHERE streamtype=%s and streamname=%s and ongoing=TRUE", (datetime.utcfromtimestamp(lastAnalysis["timestamp"]), streamtype, streamname))

                else:
                    logging.error("Too many on going events? %s" % events)

            # Get latest results from mongo
            events = db.streamResults.find({"start": {"$gte": lastPush["timestamp"]}})
            discoStream = defaultdict(list) 
            for event in events:

                ongoing = False
                if event["duration"] == -1:
                    ongoing = True

                elif event["duration"] < 900:
                    #ignore events shorter than 15 minutes
                    # print("Ignoring new results: %s" % event)
                    continue
                
                # print("Adding new results: %s" % event)
                count[event["streamType"]]+=1

                # push event to the webserver
                startDate = datetime.utcfromtimestamp(event["start"])
                avgLevel=0
                if ongoing:
                    avgLevel = 12
                    #TODO use the probeinfo here?

                    endDate = datetime.utcfromtimestamp(lastAnalysis["timestamp"])
                else:
                    # compute the average burst level
                    avgLevel = np.mean([p["state"] for p in event["probeInfo"]])
                    endDate = datetime.utcfromtimestamp(event["end"])
                cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime, avglevel, nbdiscoprobes, totalprobes, ongoing) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (event["streamType"], event["streamName"], startDate, endDate, avgLevel, len(event["probeInfo"]), event["numberOfProbesInUnit"], ongoing))
                # cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime) VALUES (%s, %s, %s, %s) RETURNING id", (event["streamType"], event["streamName"], startDate, endDate))

                eventId = cursor.fetchone()[0]

                # Add corresponding probes infos
                for probe in event["probeInfo"]:
                    cursor.execute("INSERT INTO ihr_disco_probes (probe_id, event_id, starttime, endtime, level) \
                            VALUES (%s, %s, %s, %s, %s) ", (probe["probeID"], eventId, datetime.utcfromtimestamp(probe["start"]), datetime.utcfromtimestamp(probe["end"]), probe["state"] ))

                discoStream[event["streamName"]].append( (event["start"], event["end"]) )
                    
            # Update all analyzed streams
            for asn in analyzedAsn:
                addZeros(cursor, asn, "asn", discoStream, lastPush, lastAnalysis, len(streamInfo["streamsMonitored"]["ases"][asn])) 

            for country in analyzedCountries:
                addZeros(cursor, country, "country", discoStream, lastPush, lastAnalysis, len(streamInfo["streamsMonitored"]["countries"][country])) 

            conn.commit()
            cursor.close()
            conn.close()

            db.frontEnd.insert_one({"timestamp": lastAnalysis["timestamp"]})
            for key, count in count.iteritems():
                logging.info("Added %s %s events" % (count, key) )
            logging.info("Good-bye!")


    except Exception as e: 
        tb = traceback.format_exc()
        save_note = "Exception dump: %s : %s.\nCommand: %s\nTraceback: %s" % (type(e).__name__, e, sys.argv, tb)
        exception_fp = open("dump_%s.err" % datetime.now(), "w")
        exception_fp.write(save_note) 
        sendMail(save_note)

