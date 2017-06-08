import numpy as np

inFile = "disco_trr.txt"
# inFile = "disco_trr2015.txt"

thresh = 0.5

# found only by trinocular
trino_ref=np.array([1.0,1.03,0.96,1.03,1.09,1.0,1.0,0.94,1.0,1.14,1.21,1.4,1.14,1.0,1.0,1.0,0.59,0.94,1.0,1.14,1.62,0.97,0.39,0.93,1.0,0.0,1.0,0.8,1.0,0.93,1.04,0.9,1.23])
trino_calc=np.array([0.07,0.03,0.92,0.22,0.06,1.0,0.0,0.03,0.1,0.1,0.09,0.06,0.5,0.06,1.0,1.0,0.05,0.1,0.0,0.09,0.53,0.69,0.19,0.0,0.06,0.0,0.03,0.11,1.0,0.59,0.1,0.18,0.47])

# remove cases where the ref is 0
trino_calc = trino_calc[trino_ref!=0]
# number of outages found by trinocular but not disco
fn = np.sum(trino_calc<thresh)

print "Trinocular:"
print "number of alarms: %s" % len(trino_calc)
print "TP: %s" % np.sum(trino_calc<thresh)
print "FP: %s" % np.sum(trino_calc>=thresh)

nor = []
out = []

for line in open(inFile):
    if line.startswith("R_normal"):
        continue

    wn, wo = line.split(",")

    nor.append(float(wn))
    out.append(float(wo))

out = np.array(out)
tp = np.sum(out<thresh)
fp = np.sum(out>=thresh)
# print out[out>=thresh]

print "\nDisco:"
print "number of alarms: %s" % len(out)
print "TP: %s" % tp
print "FP: %s" % fp
print "FN: %s" % fn


print "Precision: %s" % (tp/float(tp+fp))
print "Recall : %s" % (tp/float(tp+fn))
