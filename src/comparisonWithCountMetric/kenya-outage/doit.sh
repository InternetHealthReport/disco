#!/bin/bash

# this converts the file Anant gave me to something i can gnuplot
# data is in the burst-to-steps.py code
./burst-to-steps.py ./burst.data > ./stepped-burst.txt
./burst-to-steps.py ./burst-all.data > ./stepped-burst-all.txt

# this takes the probe-archive data as starting point (so goes to 2017-01-16T02)
# ... needed to figure out what the absolute number of connected probes is
./probe-connected-counts-over-time.py 2016-06-07 2016-06-08 KE > probe-connected-abs.txt
./all-probe-connected-counts-over-time.py 2016-06-07 2016-06-08 > all-probe-connected-abs.txt

#kenya signal only
gnuplot <  plot-burst
# all signal (at same time)
gnuplot <  plot-all
