import pickle
import numpy as np
from plotFunctions import plotter

succTRInfo=pickle.load(open('results/successfullTraceroutesOutageIDToMsmID.pickle','r'))
#print(succTRInfo)
msmsThatSucceded={}
msmsThatSuccededMinDur={}
totalTR=0
for oid, oinfo in succTRInfo.items():
    if oid not in [400,446]:
        continue
    for msm,durList in oinfo.items():
        if msm not in msmsThatSucceded:
            #if msm == 5010:
            #    print(oid)
            msmsThatSucceded[msm]=len(durList)
        else:
            msmsThatSucceded[msm] += len(durList)
        avgDur=np.average(durList)
        if msm not in msmsThatSuccededMinDur:
            msmsThatSuccededMinDur[msm]=avgDur
        else:
            if msmsThatSuccededMinDur[msm]>avgDur:
                msmsThatSuccededMinDur[msm]=avgDur
        totalTR += len(durList)

print("Total TR seen: {0}".format(totalTR))


cnt=0
for k,v in msmsThatSucceded.items():
    per=float(v)/totalTR*100
    print(str(per)+" "+str(k)+" "+str(msmsThatSuccededMinDur[k]))
    cnt+=per

#plotter=plotter()
#plotter.suffix='Both'
#plotter.ecdf(msmsThatSuccededMinDur.values(),'deta',xlabel='Minutes from estimated outage start/end')

