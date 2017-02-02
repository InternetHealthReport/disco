#!/bin/bash

# this converts the file Anant gave me to something i can gnuplot
# data is in the burst-to-steps.py code
./burst-to-steps.py ./burst.data > ./stepped-burst.txt

# this takes the probe-archive data as starting point (so goes to 2017-01-16T02)
# ... needed to figure out what the absolute number of connected probes is
./probe-connected-counts-over-time-asn.py 2014-08-27 2014-08-28 7843,10796,11351,11426,11427,12271,20001 > probe-connected-abs.txt

gnuplot <  plot-burst
