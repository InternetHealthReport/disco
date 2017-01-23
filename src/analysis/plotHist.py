from matplotlib import pylab as plt
import numpy as np

inFile = "disco_trr.txt"
# nor = [1.0, 1.0, 0.96, 0.9, 0.98, 1.0, 0.9, 1.0, 0.95, 1.07, 0.9, 0.96, 1.02, 1.0, 1.07, 1.0, 1.03, 1.0, 1.28, 0.82, 1.21, 0.97, 1.0, 1.13, 1.06, 1.02, 1.16, 1.0, 4.14, 1.0, 0.98, 1.02, 1.04, 1.36, 1.03, 1.3, 1.05, 0.98, 0.91, 1.17, 0.82, 0.97, 1.0, 1.2, 1.02, 0.97, 0.97, 0.98, 0.98, 1.65, 1.0, 0.85, 1.0, 0.97, 0.97, 1.12, 0.94, 0.98, 1.03, 1.42, 1.32, 0.98, 1.17, 0.97, 1.08, 0.86, 0.95, 1.0]
# out =  [0.02, 0.35, 0.07, 0.02, 0.02, 0.02, 0.04, 0.44, 0.02, 0.09, 0.09, 0.02, 0.0, 0.11, 0.19, 0.33, 0.14, 0.37, 0.12, 0.04, 0.09, 0.17, 0.39, 0.02, 0.21, 0.11, 0.07, 0.08, 0.0, 0.39, 0.21, 0.12, 0.1, 0.02, 0.62, 0.02, 0.25, 0.24, 0.11, 0.15, 0.04, 0.59, 0.33, 0.16, 0.02, 0.1, 0.0, 0.26, 0.25, 0.24, 0.15, 0.08, 0.03, 0.4, 0.06, 0.02, 0.0, 0.12, 0.08, 0.35, 0.14, 0.02, 0.21, 0.02, 0.02, 0.25, 0.03, 0.04]
nor=[]
out=[]

for line in open(inFile):
    if line.startswith("R_normal"):
        continue

    wn, wo = line.split(",")

    nor.append(float(wn))
    out.append(float(wo))


weights_nor = np.ones_like(nor)/len(nor)
weights_out = np.ones_like(out)/len(out)

plt.figure(figsize=(5,3))
plt.hist(nor, range=[0,2], bins=41, weights=weights_nor, histtype="stepfilled", label="Normal",)
plt.hist(out, range=[0,2], bins=41, weights=weights_out, color="red", histtype="stepfilled", label="Outage",hatch="////")
plt.ylabel("Probability Mass Function")
plt.xlabel("$R$ (Average Velocity Ratio)")
plt.legend()
plt.tight_layout()
plt.show()
plt.savefig("figures/R_normal_outage.eps")
