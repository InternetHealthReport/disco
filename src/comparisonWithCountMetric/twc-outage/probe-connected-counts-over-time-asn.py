#!/usr/bin/env python
import subprocess
import requests
import arrow
from ripe.atlas.cousteau import ProbeRequest, AtlasRequest
import ujson as json
import sys
import re
import netaddr

START=sys.argv[1]
END=sys.argv[2]
ASNS=set( map(int, sys.argv[3].split(',') ) )

probes = {}

# take day-1
ARCHIVE_DAY_O = arrow.get( START ).replace(days=-1)
END_O = arrow.get( END )

url_path = "/api/v2/probes/archive/?day=%s" % ( ARCHIVE_DAY_O.format('YYYY-MM-DD') )
request = AtlasRequest(**{"url_path": url_path})
#result = namedtuple('Result', 'success response')
(is_success, response) = request.get()

conn_count = 0 
snapshot_dt = response['snapshot_datetime'] # 1485483603
for p in response['results']:
   if not p['asn_v4'] in ASNS:
      continue
   probes[ p['id'] ] = p
   if p['status']['id'] == 1:
      conn_count += 1

print >>sys.stderr, "connnected: %s" % ( conn_count )

#proc = subprocess.Popen("msmfetch 7000 %s %s" % (snapshot_dt,END), shell=True,stdout=subprocess.PIPE)
api_call="https://atlas.ripe.net/api/v2/measurements/7000/results?start=%s&stop=%s&format=txt" % ( ARCHIVE_DAY_O.timestamp, END_O.timestamp )
print >>sys.stderr, "now fetching %s" % ( api_call )
r = requests.get(api_call)
if r.status_code != 200:
   print >>sys.stderr, "Received status code "+str(r.status_code)+" from "+api_call
   sys.exit(-1)

series = {}

#prb_cache = set()

#for line in iter(proc.stdout.readline,''):
for line in r.text.splitlines():
   d = json.loads( line )
   if d['prb_id'] not in probes:
      # note we don't take probes into account that just registered/changed to country
      continue
   print >>sys.stderr, d
   # 'timestamp' and 'event'
   ts = d['timestamp']
   #ts -= ts % BATCH_SIZE
   series.setdefault( ts, {'c':0,'d':0} )
   #"event": "connect"
   #print d['event']
   if d['event'] == 'disconnect':
      series[ts]['d'] += 1
   elif d['event'] == 'connect':
      series[ts]['c'] += 1
      #print d
   else:
      print >>sys.stderr, "CANT HAPPEN"

ts_sorted = sorted( series.keys() )
signal = conn_count
for ts in ts_sorted:
   signal = signal + series[ts]['c'] - series[ts]['d']
   
   #print "%s %s %s" % ( ts, series[ts][0], series[ts][1] )
   print "%s %s %s %s" % ( ts, signal, series[ts]['c'], series[ts]['d'] )
