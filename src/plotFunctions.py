from __future__ import division
from matplotlib import pylab as plt
import datetime as dt
import numpy as np
import traceback
import os
import threading
import  operator

class plotter():
    def __init__(self):
        self.figNum=0
        self.suffix='live'
        self.outputFormat='png'
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
            #try:
            #    command=('scp {0} chekov.netsec.colostate.edu:public_html/iij/{1}/'.format(outName,self.suffix))
            #    os.system(command)
            #except:
            #    pass
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
            binStart=int(min(data))
            binStop=int(max(data))
            #bins = numpy.linspace(binStart, binStop, 60)
            bins = range(binStart, binStop+1, 60)
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

            plt.plot(X,Y)
            plt.title(titleInfo)
            #print(dict)
            plt.ylim(0,max(Y)+5)
            #plt.xticks(XTicks,dtListTicks,rotation='80')
            fig.autofmt_xdate()
            plt.autoscale()
            plt.savefig(outName)
            #try:
            #    command=('scp {0} chekov.netsec.colostate.edu:public_html/iij/{1}/'.format(outName,self.suffix))
            #    os.system(command)
            #except:
            #    pass
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
            fig = plt.figure(num)
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
                plt.plot(val["x"], val["y"], label=q,color='c')
                plt.fill_between(val["x"], val["y"],0,color='c')

            plt.ylabel("Burst level")
            fig.autofmt_xdate()
            plt.autoscale()
            plt.savefig(outName)
            #try:
            #    command=('scp {0} chekov.netsec.colostate.edu:public_html/iij/{1}/'.format(outName,self.suffix))
            #    os.system(command)
            #except:
            #    pass
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