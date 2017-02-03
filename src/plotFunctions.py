from __future__ import division
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
#plt=matplotlib.pyplot
import datetime as dt
from datetime import datetime
import numpy as np
import traceback
import os
import pandas as pd
import threading
import  operator
from scipy.interpolate import UnivariateSpline


class plotter():
    def __init__(self):
        self.figNum=0
        self.suffix='live'
        self.outputFormat='eps'
        self.year=None
        self.month = None
        self.day = None
        self.lock = threading.RLock()

    def plotDict(self,d,outFileName):
        if len(d)==0:
            return
        self.lock.acquire()
        try:
            num=self.getFigNum()
            outName=outFileName+'_'+self.suffix+'.'+self.outputFormat
            fig = plt.figure(num)
            print('Plotting Figure {0}: {1}'.format(num,outName))
            plt.tick_params(axis='both', which='major', labelsize=7)
            X = np.arange(len(d))
            plt.bar(X, d.values(), align='center')#, width=0.5)
            plt.xticks(X, d.keys(), rotation='80')
            values=d.values()
            #print(values)
            try:
                if len(values) == 1:
                    ymax = values[0] + 1
                else:
                    ymax = max(values) + 3
            except:
                pass
            #plt.ylim(0, ymax)
            #plt.yscale('log')
            #plt.show()
            plt.autoscale()
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plotList(self,dataIn,outfileName,titleInfo=''):
        if len(dataIn)==0:
            return
        self.lock.acquire()
        try:
            #print(dataIn)
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            data = [x / 1 for x in dataIn]
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num,figsize=(10,8))
            lowestTime=min(data)
            #binStart=int(min(data))
            year, month, day = datetime.utcfromtimestamp(float(lowestTime)).strftime("%Y-%m-%d").split('-')
            #binStop=int(max(data))
            binStart=int(datetime(int(year),int(month),int(day), 0, 0).strftime('%s'))
            binStop=int(datetime(int(year), int(month), int(day), 23, 59).strftime('%s'))

            #bins = numpy.linspace(binStart, binStop, 60)
            bins = range(binStart, binStop+1, 20)
            digitized=np.digitize(data, bins)
            #print(digitized)
            dtList=[]
            for v in bins:
                dtList.append(dt.datetime.utcfromtimestamp(v))
            dict={}
            for val in digitized:
                if dtList[val-1] not in dict.keys():
                    dict[dtList[val-1]]=1
                else:
                    dict[dtList[val-1]]+=1
            #print(min(dict.keys()),max(dict.keys()))
            X=range(0,len(dict.keys())+1)
            X=sorted(dict.keys())

            #Incase if data was not spread apart enough
            #if len(X)==0:
            #    return
            #steps=int(len(X)/5)
            #if steps<1:
            #    steps=1

            #XTicks=range(0,len(X),steps)
            #dtListTicks=[]
            #for iters in XTicks:
                #print(dtList[iters])
            #    dtListTicks.append(dtList[iters])
                #print(iters,dtList[iters])

            Y=dict.values()
            plt.xlim(datetime(int(year),int(month),int(day), 0, 0),datetime(int(year),int(month),int(day), 23, 59))
            plt.plot(X,Y)
            plt.title(titleInfo)
            #print(dict)
            plt.ylim(0,max(Y)+5)
            #plt.xticks(XTicks,dtListTicks,rotation='80')
            fig.autofmt_xdate()
            plt.autoscale()
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plotPDF(self,dataIn1,outfileName,xlabel='',ylabel='',titleInfo=''):
        N = 1000
        n = N/10
        s = np.random.normal(size=N)   # generate your data sample with N elements
        p, x = np.histogram(s, bins=n) # bin it into n = N/10 bins
        x = x[:-1] + (x[1] - x[0])/2   # convert bin edges to centers
        f = UnivariateSpline(x, p, s=n)
        plt.plot(x, f(x))
        plt.show()

    def plot2ListsHist(self,dataIn1,dataIn2,outfileName,xlabel='',ylabel='',titleInfo=''):
        if len(dataIn1)==0:
            return
        if len(dataIn2)==0:
            return
        self.lock.acquire()
        try:
            #print(dataIn)
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num,figsize=(10,8))
            binStart1=0
            binStop1=int(max(dataIn1))
            binStart2=0
            binStop2=int(max(dataIn2))
            bins1 = range(binStart1, binStop1+1, 10)
            digitized1=np.digitize(dataIn1, bins1)

            bins2 = range(binStart2, binStop2+1, 10)
            digitized2=np.digitize(dataIn2, bins2)
            dtList1=[]
            for v in bins1:
                dtList1.append(dt.datetime.utcfromtimestamp(v))
            dict1={}
            for val in digitized1:
                if dtList1[val-1] not in dict1.keys():
                    dict1[dtList1[val-1]]=1
                else:
                    dict1[dtList1[val-1]]+=1

            X1=sorted(digitized1)
            Y1=dict1.values()

            dtList2=[]
            for v in bins2:
                dtList2.append(dt.datetime.utcfromtimestamp(v))
            dict2={}
            for val in digitized2:
                if dtList2[val-1] not in dict2.keys():
                    dict2[dtList2[val-1]]=1
                else:
                    dict2[dtList2[val-1]]+=1

            X2=sorted(digitized2)
            Y2=dict2.values()

            plt.plot(X1,Y1)
            plt.plot(X2,Y2)


            plt.title(titleInfo)
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
            plt.autoscale()
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plotBursts(self,bursts,name):
        if len(bursts)==0:
            print('No bursts!')
            return
        self.lock.acquire()
        try:
            outName=name+'_'+self.suffix+'.'+self.outputFormat
            num=self.getFigNum()
            fig = plt.figure(num, figsize=(15,3))
            ax = fig.add_subplot(1,1,1)
            print('Plotting Figure {0}: {1}'.format(num,outName))
            #print(bursts)
            b = {}
            for q, ts, te in bursts:
                if not q in b:
                    b[q] = {"x":[], "y":[]}

                b[q]["x"].append(dt.datetime.utcfromtimestamp(ts))
                b[q]["y"].append(0)
                b[q]["x"].append(dt.datetime.utcfromtimestamp(ts))
                b[q]["y"].append(q)
                b[q]["x"].append(dt.datetime.utcfromtimestamp(te))
                b[q]["y"].append(q)
                b[q]["x"].append(dt.datetime.utcfromtimestamp(te))
                b[q]["y"].append(0)

            for q, val in b.iteritems():
                plt.plot(val["x"], val["y"], label=q,color='#11557c')
                plt.fill_between(val["x"], val["y"],0,color='#11557c')

            plt.ylabel("Burst level")
            plt.xlim([dt.datetime(int(self.year),int(self.month),int(self.day),0,0), dt.datetime(int(self.year),int(self.month),int(self.day),23,59)])
            plt.ylim([0, 15])
            fig.autofmt_xdate()
            # plt.autoscale()
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))   #to get a tick every 15 minutes
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))     #optional formatting 
            ax.xaxis.set_minor_locator(mdates.MinuteLocator(byminute=[30]))   #to get a tick every 15 minutes
            ax.xaxis.set_minor_formatter(mdates.DateFormatter(''))     #optional formatting 
            # ax.set_xticklabels(["","08:00","","09:00","","10:00","","11:00","","12:00",""])
            plt.grid(True, which="minor", color="0.6", linestyle=":")
            plt.grid(True, which="major", color="k", linestyle=":")
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()


    def getFigNum(self):
        self.lock.acquire()
        try:
            self.figNum+=1
        except:
            traceback.print_exc()
        finally:
            self.lock.release()
        #print(self.figNum)
        return self.figNum

    def setSuffix(self,suffixName):
        self.lock.acquire()
        try:
            self.suffix=suffixName
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plotBinned(self,dataIn,binSize,outfileName,xlabel='',ylabel='',titleInfo=''):
        if len(dataIn)==0:
            return
        self.lock.acquire()
        try:
            #print(dataIn)
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            data = [x / 1 for x in dataIn]
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num,figsize=(10,8))
            #binStart=int(min(data))
            #binStop=int(max(data))
            #bins = range(binStart, binStop+1, binSize)
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
            plt.hist(data,bins=binSize)
            plt.title(titleInfo)
            plt.autoscale()
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plotDensity(self,data,outfileName,xlabel='',ylabel='',titleInfo=''):
        self.lock.acquire()
        try:
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            df = pd.DataFrame(data)
            ax=df.plot(kind='density')
            fig = ax.get_figure()
            print('Plotting Figure {0}: {1}'.format(1,outName))
            fig.savefig(outName)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plotDensities(self,data1,data2,outfileName,data1Label='',data2Label='',xlabel='',ylabel='Density',titleInfo='',xticks=None,xlim=None):
        self.lock.acquire()
        try:
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num)
            df1 = pd.DataFrame(data1)
            df1.columns = [data1Label]
            ax1=df1.plot(kind='density',xticks=xticks,xlim=xlim)
            ax1.set_xlabel(xlabel,fontsize=16)
            ax1.set_ylabel(ylabel,fontsize=16)
            df2 = pd.DataFrame(data2)
            df2.columns = [data2Label]
            df2.plot(kind='density',ax=ax1)
            fig = ax1.get_figure()
            fig.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plot2Hists(self,data1,data2,outfileName,data1Label='',data2Label='',xlabel='',ylabel='',titleInfo='',xticks=None,xlim=None):
        self.lock.acquire()
        try:
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num,figsize=(5.8,4.6))
            ax = plt.subplot(111)
            box = ax.get_position()
            ax.set_position([box.x0, box.y0 + box.height * 0.15,box.width, box.height * 0.85])

            ax.hist(data1,bins=70,range=[0,2],histtype='step',lw=2,label=data1Label)
            ax.hist(data2,bins=70,range=[0,2],histtype='step',lw=2,label=data2Label)
            plt.xlabel(xlabel,fontsize = 18)
            plt.ylabel(ylabel,fontsize = 18)
            plt.xticks(fontsize = 18)
            plt.yticks(fontsize = 18)
            ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.18),fancybox=True, shadow=True, ncol=2)
            #plt.legend()
            #plt.tight_layout()
            plt.autoscale()
            #plt.show()
            fig.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def plotHeatMap(self,data1,data2,outfileName,xlabel='',ylabel='',titleInfo='',xticks=None,xlim=None):
        self.lock.acquire()
        try:
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num)

            # Generate some test data


            #heatmap, xedges, yedges = np.histogram2d(data1,data2, bins=20)
            #extent = [0,3,0,3]
            #plt.imshow(heatmap, extent=extent)

            #plt.hexbin(data1,data2,cmap=plt.cm.Blues_r, bins=30)

            #plt.hist2d(data1,data2,cmap=plt.cm.Blues,bins=70)
            #extent = [0,3,0,3]
            #plt.imshow(heatmap, extent=extent)
            #plt.xlim(0,2)
            #plt.ylim(0,2)

            #plt.hist2d(data1,data2,bins=70);

            plt.hexbin(data1,data2, cmap="OrRd", gridsize=70, vmax=10, vmin=0, mincnt=0)

            plt.xlim([-0.1,2])
            plt.ylim([-0.1,2])
            plt.colorbar()

            plt.xlabel(xlabel)
            plt.ylabel(ylabel)


            plt.xticks(fontsize = 18)
            plt.yticks(fontsize = 18)
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def ecdf(self,data,outfileName,xlabel='',ylabel='CDF',titleInfo='',xlim=[]):
        self.lock.acquire()
        try:
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num, figsize=(4,3))
            plt.xlabel(xlabel) #,fontsize=18)
            plt.ylabel(ylabel) #,fontsize=18)
            plt.tick_params() #labelsize=18)
            if len(xlim)>0:
                plt.xlim(xlim)
            plt.grid()
            plt.title(titleInfo) #,fontsize=16)
            sorted=np.sort(data)
            yvals=np.arange(len(sorted))/float(len(sorted))
            plt.plot( sorted, yvals,lw=2)
            plt.autoscale()
            plt.tight_layout()
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def ecdfs(self,data1,data2,outfileName,xlabel='',ylabel='',titleInfo=''):
        self.lock.acquire()
        try:
            outName=outfileName+'_'+self.suffix+'.'+self.outputFormat
            num=self.getFigNum()
            print('Plotting Figure {0}: {1}'.format(num,outName))
            fig = plt.figure(num,figsize=(8,6))
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
            plt.title(titleInfo)
            plt.grid()
            #data1
            sorted1=np.sort(data1)
            yvals=np.arange(len(sorted1))/float(len(sorted1))
            plt.plot(sorted1, yvals)
            #data2
            sorted2=np.sort(data2)
            yvals=np.arange(len(sorted2))/float(len(sorted2))
            plt.plot(sorted2, yvals)
            plt.autoscale()
            plt.savefig(outName)
            plt.close(fig)
        except:
            traceback.print_exc()
        finally:
            self.lock.release()
