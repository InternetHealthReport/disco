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

MINEVENTLENGTH = 600

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


if __name__ == "__main__":
    try:
        FORMAT = '%(asctime)s %(processName)s %(message)s'
        logging.basicConfig(format=FORMAT, filename='push2frontEnd.log', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
        logging.info("Started: %s" % sys.argv)

        # Check if there is new results
        client = pymongo.MongoClient("mongodb-iijlab",connect=True)
        db = client.disco
        lastPush = db.frontEnd.find().sort("timestamp", pymongo.DESCENDING).limit(1)[0]
        lastAnalysis = db.streamResults.find().sort("insertTime", pymongo.DESCENDING).limit(1)[0]
        count = defaultdict(int)
        ongoingEventsId = []
        
        # Check if disco analyzed more data
        if lastPush["timestamp"] != lastAnalysis["insertTime"]:
            logging.info("Time window shifted: %s -> %s" % (lastPush["timestamp"], lastAnalysis["insertTime"]))
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
                    insert into ihr_asn(number, name, disco, tartiflette, ashash) values(%s, %s, TRUE, FALSE, FALSE);
                  exception when unique_violation then
                    update ihr_asn set disco = TRUE where number = %s;
                end $$;""", (asn, asNames["AS"+str(asn)], asn))
                conn.commit()
                

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
            cursor.execute("SELECT id, mongoid FROM ihr_disco_events WHERE ongoing=TRUE")
            ongoingEvents = cursor.fetchall()

            for eventidpg, eventidmg in ongoingEvents:
                ongoingEventsId.append(eventidmg)
                events = db.streamResults.find({"_id": objectid.ObjectId(eventidmg)})
                events = list(events)
                if len(events)>1:
                    logging.error("Too many on going events? %s" % events)

                if len(events)>0 and events[0]["insertTime"] > lastPush["timestamp"]:
                    event = events[0]
                    #update website
                    if event["duration"] >= MINEVENTLENGTH or event["duration"]==-1:
                        avgLevel = np.mean([p["state"] for p in event["probeInfo"]])
                        ongoing = "FALSE"
                        if event["duration"]==-1:
                            ongoing = "TRUE"

                        cursor.execute("UPDATE ihr_disco_events SET endtime=%s, avglevel=%s, nbdiscoprobes=%s, ongoing=%s \
                            WHERE mongoid=%s ", (datetime.utcfromtimestamp(event["end"]), avgLevel, len(event["probeInfo"]), ongoing, eventidmg))

                        # update probe end time here: 
                        cursor.execute("UPDATE ihr_disco_probes SET endtime=%s WHERE event_id=%s", (datetime.utcfromtimestamp(event["end"]), eventidpg ))
                        # add the rest of probes:
                        for probe in event["probeInfo"]:
                            cursor.execute("INSERT INTO ihr_disco_probes (probe_id, event_id, starttime, endtime, level, ipv4, prefixv4) \
                                    VALUES (%s, %s, %s, %s, %s, %s, %s) ", (probe["probeID"], eventidpg, datetime.utcfromtimestamp(probe["start"]), datetime.utcfromtimestamp(probe["end"]), probe["state"], probe["address_v4"], probe["prefix_v4"] ))

                    else:
                        # remove event if duration < 15min
                        cursor.execute("DELETE from ihr_disco_events WHERE mongoid=%s", ( eventidmg, ))
                        cursor.execute("DELETE from ihr_disco_probes WHERE event_id=%s", ( eventidpg, ))

                else:
                    #event still ongoing
                    # event = events[0]
                    # avgLevel = np.mean([p["state"] for p in event["probeInfo"]])
                    # TODO update avglevel?
                    cursor.execute("UPDATE ihr_disco_events SET endtime=%s \
                        WHERE mongoid=%s", (datetime.utcfromtimestamp(lastAnalysis["insertTime"]), eventidmg))

            # Get latest results from mongo
            logging.info("Getting new events from mongo")
            events = db.streamResults.find({"insertTime": {"$gte": lastPush["timestamp"]}})
            for event in events:
                logging.info(event)
                if event["_id"] in ongoingEventsId:
                    # Skip ongoing events, they are already in the db
                    continue

                ongoing = False
                if event["duration"] == -1:
                    ongoing = True

                elif event["duration"] < MINEVENTLENGTH:
                    #ignore events shorter than 15 minutes
                    # print("Ignoring new results: %s" % event)
                    continue
                
                # print("Adding new results: %s" % event)
                count[event["streamType"]]+=1

                # push event to the webserver
                startDate = datetime.utcfromtimestamp(event["start"])
                avgLevel=0
                if ongoing:
                    avgLevel = np.mean([p["state"] for p in event["probeInfo"]])
                    endDate = datetime.utcfromtimestamp(lastAnalysis["insertTime"])
                else:
                    # compute the average burst level
                    avgLevel = np.mean([p["state"] for p in event["probeInfo"]])
                    endDate = datetime.utcfromtimestamp(event["end"])
                cursor.execute("INSERT INTO ihr_disco_events (mongoid, streamtype, streamname, starttime, endtime, avglevel, nbdiscoprobes, totalprobes, ongoing) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (str(event["_id"]), event["streamType"], event["streamName"], startDate, endDate, avgLevel, len(event["probeInfo"]), event["numberOfProbesInUnit"], ongoing))
                # cursor.execute("INSERT INTO ihr_disco_events (streamtype, streamname, starttime, endtime) VALUES (%s, %s, %s, %s) RETURNING id", (event["streamType"], event["streamName"], startDate, endDate))

                eventId = cursor.fetchone()[0]

                # Add corresponding probes infos
                for probe in event["probeInfo"]:
                    cursor.execute("INSERT INTO ihr_disco_probes (probe_id, event_id, starttime, endtime, level, ipv4, prefixv4) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s) ", (probe["probeID"], eventId, datetime.utcfromtimestamp(probe["start"]), datetime.utcfromtimestamp(probe["end"]), probe["state"], probe["address_v4"], probe["prefix_v4"] ))

            conn.commit()
            cursor.close()
            conn.close()

            db.frontEnd.insert_one({"timestamp": lastAnalysis["insertTime"]})
            for key, count in count.iteritems():
                logging.info("Added %s %s events" % (count, key) )
            logging.info("Good-bye!")

        else:
            logging.info("Time window hasn't moved: lastPush = %s lastAnalysis= %s" % (lastPush["timestamp"], lastAnalysis["insertTime"]))

    except Exception as e: 
        tb = traceback.format_exc()
        save_note = "Exception dump: %s : %s.\nCommand: %s\nTraceback: %s" % (type(e).__name__, e, sys.argv, tb)
        exception_fp = open("dump_%s.err" % datetime.now(), "w")
        exception_fp.write(save_note) 
        sendMail(save_note)

