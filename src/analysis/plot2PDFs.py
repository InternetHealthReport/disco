from plotFunctions import plotter

if __name__ == "__main__":
    '''
    plotter=plotter()
    plotter.suffix='FTRBoth'


    refFTR=[1.0,1.04,1.1,1.3,1.04,0.97,1.32,0.96,1.16,1.04,1.29,1.07,1.0,1.0,0.9,1.0,1.0,1.0,1.15,1.48,0.69,0.75,0.0,1.0,0.89,1.06,0.95,1.27,1.0,1.57,1.0,1.13,0.0,0.93,1.24,0.57,1.0,0.62,0.96,1.0,1.37,1.35,1.19,1.17,1.09,1.12,1.04,1.14,1.0,0.88,0.0,1.25,0.52,1.0,1.17,0.0,1.0,1.04,1.13,0.84,1.03,0.96,0.94,0.72,0.75,0.0,1.08,1.2,1.13,0.71,1.32,1.08,1.0,0.0]
    calFTR=[1.73,1.92,0.14,0.26,0.04,0.05,0.23,1.31,0.05,1.44,0.5,1.39,0.05,0.7,0.68,1.14,0.74,1.04,0.36,0.07,0.33,0.67,6.94,0.82,0.04,2.22,0.43,0.12,0.76,0.22,1.05,0.79,0.07,1.38,1.21,0.19,1.09,0.04,1.09,1.08,0.3,2.57,0.28,2.67,1.83,0.35,1.68,2.17,0.05,0.61,0.69,0.5,0.97,0.95,1.18,0.06,0.05,1.25,3.62,1.69,0.33,0.82,2.77,1.17,0.04,0.22,2.23,0.36,1.0,2.47,1.44,0.23,1.76,0.4]
    xticks=[0,1,2]
    xlim=[0,2]
    plotter.plotDensities(refFTR,calFTR,'figures/TRRs_PDF',data1Label='TRR_normal',data2Label='TRR_outage',xlabel='Traceroute Rate Ratio',xlim=xlim,xticks=xticks)
    plotter.plot2Hists(refFTR,calFTR,'figures/TRRs_hist',data1Label='TRR_normal',data2Label='TRR_outage',xlabel='Traceroute Rate Ratio',ylabel='Number of Outages')


    plotter.suffix='Both'
    '''
    ref=[1.0,1.0,0.96,0.9,0.98,1.0,0.9,1.0,0.95,1.07,0.9,0.96,1.02,1.0,1.07,1.0,1.03,1.0,1.28,0.82,1.21,0.97,0.0,1.0,1.13,1.06,1.02,1.16,1.0,4.14,1.0,0.98,0.0,1.02,1.04,1.36,1.03,1.3,1.05,0.98,0.91,1.17,0.82,0.97,1.0,1.2,1.02,0.97,0.97,0.98,0.0,0.98,1.65,1.0,0.85,0.0,1.0,0.97,0.97,1.12,0.94,0.98,1.03,1.42,1.32,0.0,0.98,1.17,0.97,1.08,0.86,0.95,1.0,0.0]
    cal=[0.02,0.35,0.07,0.02,0.02,0.02,0.04,0.44,0.02,0.09,0.09,0.02,0.0,0.11,0.19,0.33,0.14,0.37,0.12,0.04,0.09,0.17,0.11,0.39,0.02,0.21,0.11,0.07,0.08,0.0,0.39,0.21,0.0,0.12,0.1,0.02,0.62,0.02,0.25,0.24,0.11,0.15,0.04,0.59,0.33,0.16,0.02,0.1,0.0,0.26,0.25,0.25,0.24,0.15,0.08,0.0,0.03,0.4,0.06,0.02,0.0,0.12,0.08,0.35,0.14,0.1,0.02,0.21,0.02,0.02,0.25,0.03,0.04,0.13]

    newref=[]
    newcal=[]
    for itr in range(0,len(ref)):
        if ref[itr]!=0:
            newref.append(ref[itr])
            newcal.append(cal[itr])

    #plotter.plotDensities(ref,cal,'figures/TRRs_PDF',data1Label='TRR_normal',data2Label='TRR_outage',xlabel='Traceroute Rate Ratio',xlim=xlim,xticks=xticks)
    #plotter.plot2Hists(ref,cal,'figures/TRRs_hist',data1Label='TRR_normal',data2Label='TRR_outage',xlabel='Traceroute Rate Ratio',ylabel='Number of Outages')


    print(len(ref),len(cal))
    print(newref,newcal)

    #cat outageEval/outageEval11WithLen.txt| awk -F'|' '{print$5}' | grep -v NA | grep -v t | tr '\n' ,
