#!/usr/bin/env python
import sys

s = {}

in_file = sys.argv[1]

#for line in data.split('\n'):
with open(in_file,'r') as inf:
   for line in iter(inf):
      lvl,start,end = map(int, map(float, line.split() ) )
      s.setdefault( start, set() )
      s.setdefault( end, set() )
      s[ start ].add( (lvl, 's') )
      s[ end ].add( (lvl, 'e') )

for key in sorted( s.keys() ):
   print "# %s %s " % ( key, s[key] )
   max_s = -1
   max_e = -1
   for x in s[key]:
      if x[1] == 's' and x[0] > max_s:
         max_s = x[0]
      if x[1] == 'e' and x[0] > max_e:
         max_e = x[0]
   if max_s > 0:
      print "%s %s" % (key, max_s)
   else:
      print "%s %s" % (key, max_e - 1 )
