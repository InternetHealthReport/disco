from ripe.atlas.cousteau import AtlasStream
import textwrap
import smtplib
import datetime
import time
import datetime 
import sys


class ConnectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def on_result_response(*args):
    """
    Function called every time we receive a new traceroute.
    Store the traceroute in the corresponding Mongodb collection.
    """

    trace = args[0]
    print trace


def on_error(*args):
    print "got in on_error"
    print args

    raise ConnectionError("Error")


def on_connect(*args):
    print "got in on_connect"
    print args

def on_reconnect(*args):
    print "got in on_reconnect"
    print args

    raise ConnectionError("Reconnection")

def on_close(*args):
    print "got in on_close"
    print args

    raise ConnectionError("Closed")

def on_disconnect(*args):
    print "got in on_disconnect"
    print args

    raise ConnectionError("Disconnection")


def on_connect_error(*args):
    print "got in on_connect_error"
    print args

    raise ConnectionError("Connection Error")

def on_atlas_error(*args):
    print "got in on_atlas_error"
    print args


def on_atlas_unsubscribe(*args):
    print "got in on_atlas_unsubscribe"
    print args
    raise ConnectionError("Unsubscribed")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "usage: %s id0 [ id1 [id2 [...]]]" % sys.argv[0]
        sys.exit()

    #Start time of this script, we'll try to get it working for 1 hour
    starttime = datetime.datetime.now()


    lastTimestamp = 0
    currCollection = None
    lastDownload = None
    lastConnection = None

    allmsm = []
    for msmId in sys.argv[1:]:
        allmsm.append(int(msmId))


    while (datetime.datetime.now()-starttime).seconds < 3600:
        try:
            lastConnection = datetime.datetime.now()
            atlas_stream = AtlasStream()
            atlas_stream.connect()
            # Measurement results
            channel = "atlas_result"
            # Bind function we want to run with every result message received
            atlas_stream.socketIO.on("connect", on_connect)
            atlas_stream.socketIO.on("disconnect", on_disconnect)
            atlas_stream.socketIO.on("reconnect", on_reconnect)
            atlas_stream.socketIO.on("error", on_error)
            atlas_stream.socketIO.on("close", on_close)
            atlas_stream.socketIO.on("connect_error", on_connect_error)
            atlas_stream.socketIO.on("atlas_error", on_atlas_error)
            atlas_stream.socketIO.on("atlas_unsubscribed", on_atlas_unsubscribe)
            # Subscribe to new stream 
            atlas_stream.bind_channel(channel, on_result_response)
            
            for msm in allmsm:
                # stream_parameters = {"type": "traceroute", "buffering":True, "equalsTo":{"af": 4},   "msm": msm}
                stream_parameters = { "buffering":True, "equalsTo":{"af": 4},   "msm": msm}
                atlas_stream.start_stream(stream_type="result", **stream_parameters)

            # Run for 1 hour
            print "start stream for msm ids: %s" % allmsm
            atlas_stream.timeout(seconds=3600-(datetime.datetime.now()-starttime).seconds)
            # Shut down everything
            atlas_stream.disconnect()
            break

        except ConnectionError as e:
            now = datetime.datetime.utcnow()
            print "%s: %s" % (now, e)
            print "last download: %s" % lastDownload
            print "last connection: %s" % lastConnection
            atlas_stream.disconnect()

            # Wait a bit if the connection was made less than a minute ago
            if lastConnection + datetime.timedelta(60) > now:
                time.sleep(60) 
            print "Go back to the loop and reconnect"

        except Exception as e: 
            save_note = "Exception dump: %s : %s.\nCommand: %s" % (type(e).__name__, e, sys.argv)
            exception_fp = open("dump_%s.err" % datetime.datetime.now(), "w")
            exception_fp.write(save_note) 
            sys.exit()
