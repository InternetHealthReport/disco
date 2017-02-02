#!/bin/bash

# this converts the file Anant gave me to something i can gnuplot
# data is in the burst-to-steps.py code
./burst-to-steps.py ./burst.data > ./stepped-burst.txt

# this takes the probe-archive data as starting point (so goes to 2017-01-16T02)
# ... needed to figure out what the absolute number of connected probes is
./probe-connected-counts-over-time.py 2011-07-28 2011-07-29 NL > probe-connected-abs.txt

gnuplot <  plot-burst
