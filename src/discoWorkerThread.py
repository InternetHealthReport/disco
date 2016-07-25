
def workerD():
    global intDisControllerDict
    global intDisASNDict
    global intDisCountryDict
    global intDisProbeIDDict
    global numProbesInUnit
    while True:
        eventLocal=[]
        time.sleep(WAIT_TIME)

        itemsToRead=dataQueueDisconnect.qsize()
        itr2=itemsToRead
        #print('Dis '+str(itemsToRead))
        if itemsToRead>1:
            while itemsToRead:
                event=dataQueueDisconnect.get()
                eventLocal.append(event)
                itemsToRead-=1
                #dataQueueDisconnect.task_done()

            interestingEvents=getFilteredEvents(eventLocal)
            signalMapCountries=getUniqueSignalInEvents(interestingEvents)


            for key,probeIDSet in signalMapCountries.items():
                numProbesInUnit=0
                #print(key,probeIDSet)
                asnKey=False
                countryKey=False
                try:
                    asn=int(key)
                    numProbesInUnit=len(probeInfo.asnToProbeIDDict[asn])
                    asnKey=True
                except:
                    numProbesInUnit=len(probeInfo.countryToProbeIDDict[key])
                    countryKey=True

                if numProbesInUnit < MIN_PROBES:
                    continue

                probesInFilteredData=set()
                tsClean=[]
                eventClean=[]
                #Manage duplicate values
                timestampDict={}
                for eventVal in interestingEvents:
                    pID=int(eventVal['prb_id'])
                    asn=None
                    try:
                        asn=int(eventVal['asn'])
                    except:
                        pass
                    if (asnKey and key==asn) or (countryKey and key==probeInfo.probeIDToCountryDict[pID]):
                        if pID in probeIDSet:
                            #print('Valid Probe ID for {0}'.format(key))
                            tStamp=float(eventVal['timestamp'])
                            eventVal['timestamp']=tStamp
                            if tStamp not in timestampDict.keys():
                                timestampDict[tStamp]=1
                            else:
                                timestampDict[tStamp]+=1
                            eventClean.append(eventVal)
                            probesInFilteredData.add(pID)

                for tStamp,numOfRep in timestampDict.items():
                    for gr in range(1,(numOfRep+1)):
                        tsClean.append((tStamp)+(gr/numOfRep))

                if len(tsClean)<=SIGNAL_LENGTH:
                    continue

                tsClean.sort()
                if rawDataPlot:
                    titleInfoText='Total probes matching filter: {0}\nNumber of probes seen in disconnection events: {1}'.format(numProbesInUnit,len(probesInFilteredData))
                    plotter.plotList(tsClean,'figures/disconnectionRawData_'+str(key),titleInfo=titleInfoText)
                #print(tsClean)
                bursts = kleinberg(tsClean,timeRange=dataTimeRangeInSeconds,probesInUnit=numProbesInUnit)
                if burstDetectionPlot:
                    plotter.plotBursts(bursts,'figures/disconnectionBursts_'+str(key))

                burstsDict={}
                for brt in bursts:
                    q=brt[0]
                    qstart=brt[1]
                    qend=brt[2]
                    if q not in burstsDict.keys():
                        burstsDict[q]=[]
                    tmpDict={'start':float(qstart),'end':float(qend)}
                    burstsDict[q].append(tmpDict)

                thresholdedEvents=applyBurstThreshold(burstsDict,eventClean)
                print('dis',key,len(thresholdedEvents),len(eventClean))

                if len(thresholdedEvents)>0:

                    intDisControllerDict=groupByController(thresholdedEvents)
                    intDisProbeIDDict=groupByProbeID(thresholdedEvents)
                    intDisASNDict=groupByASN(thresholdedEvents)
                    intDisCountryDict=groupByCountry(thresholdedEvents)
                    with closing(open('data/ne/choroDisData.txt','w')) as fp:
                        print("CC,DISCON",file=fp)
                        for k,v in intDisCountryDict.items():
                            probesOwned=len(probeInfo.countryToProbeIDDict[k])
                            probesInEvent=v
                            #Some probes could be added after the probe enrichment data was grabbed
                            if probesInEvent > probesOwned:
                                #print(k,v,probesOwned)
                                normalizedVal=1
                            else:
                                normalizedVal=float(probesInEvent)/probesOwned
                            #print(v,normalizedVal)
                            print("{0},{1}".format(k,normalizedVal),file=fp)
                            #Hack to make sure data has atleast 2 elements
                            if len(intDisCountryDict)==1:
                                if 'MC' not in intDisCountryDict.keys():
                                    print("{0},{1}".format('MC',0),file=fp)
                                elif 'VA' not in intDisCountryDict.keys():
                                    print("{0},{1}".format('VA',0),file=fp)
                            elif len(intDisCountryDict)==0:
                                print("{0},{1}".format('MC',0),file=fp)
                                print("{0},{1}".format('VA',0),file=fp)

                    plotter.lock.acquire()
                    try:
                        if groupByCountryPlot and not countryKey:
                            plotter.plotDict(intDisCountryDict,'figures/disconnectionsByCountry_'+str(key))
                        if groupByASNPlot and not asnKey:
                            plotter.plotDict(intDisASNDict,'figures/disconnectionsByASN_'+str(key))
                        if groupByControllerPlot:
                            plotter.plotDict(intDisControllerDict,'figures/disconnectionsByController_'+str(key))
                        if groupByProbeIDPlot:
                            plotter.plotDict(intDisProbeIDDict,'figures/disconnectionsByProbeID_'+str(key))
                        if choroplethPlot:
                            #if len(intDisCountryDict) > 1:
                            plotChoropleth('data/ne/choroDisData.txt','figures/disconnectionChoroPlot_'+str(key)+'_'+plotter.suffix+'.png',plotter.getFigNum())
                        copyToServerFunc('dis')

                        intDisControllerDict.clear()
                        intDisProbeIDDict.clear()
                        intDisASNDict.clear()
                        intDisCountryDict.clear()
                    except:
                        traceback.print_exc()
                    finally:
                        plotter.lock.release()

            for iter in range(0,itr2):
                dataQueueDisconnect.task_done()

def workerC():
    global intConCountryDict
    global intConControllerDict
    global intConASNDict
    global intConProbeIDDict
    global numProbesInUnit

    while True:
        eventLocal=[]
        time.sleep(WAIT_TIME)
        itemsToRead=dataQueueConnect.qsize()
        itr2=itemsToRead
        #print('Con '+str(itemsToRead))
        if itemsToRead>1:
            while itemsToRead:
                event=dataQueueConnect.get()
                eventLocal.append(event)
                itemsToRead-=1
                #dataQueueConnect.task_done()

            interestingEvents=getFilteredEvents(eventLocal)
            signalMapCountries=getUniqueSignalInEvents(interestingEvents)

            #Manage duplicate values
            for key,probeIDSet in signalMapCountries.items():
                numProbesInUnit=0
                asnKey=False
                countryKey=False
                #print(key,probeIDSet)
                try:
                    asn=int(key)
                    numProbesInUnit=len(probeInfo.asnToProbeIDDict[asn])
                    asnKey=True
                except:
                    numProbesInUnit=len(probeInfo.countryToProbeIDDict[key])
                    countryKey=True

                if numProbesInUnit < MIN_PROBES:
                    continue

                timestampDict={}
                eventClean=[]
                tsClean=[]
                probesInFilteredData=set()
                for eventVal in interestingEvents:
                    pID=int(eventVal['prb_id'])
                    asn=None
                    try:
                        asn=int(eventVal['asn'])
                    except:
                        pass
                    if (asnKey and key==asn) or (countryKey and key==probeInfo.probeIDToCountryDict[pID]):
                        if pID in probeIDSet:
                            tStamp=float(eventVal['timestamp'])
                            eventVal['timestamp']=tStamp
                            if tStamp not in timestampDict.keys():
                                timestampDict[tStamp]=1
                            else:
                                timestampDict[tStamp]+=1
                            eventClean.append(eventVal)
                            probesInFilteredData.add(pID)


                for tStamp,numOfRep in timestampDict.items():
                    for gr in range(1,numOfRep+1):
                        tsClean.append((tStamp)+(gr/numOfRep))

                if len(tsClean)<SIGNAL_LENGTH:
                    continue

                tsClean.sort()
                #print(tsClean)
                if rawDataPlot:
                    titleInfoText='Total probes matching filter: {0}\nNumber of probes seen in connection events: {1}'.format(numProbesInUnit,len(probesInFilteredData))
                    plotter.plotList(tsClean,'figures/connectionRawData_'+str(key),titleInfo=titleInfoText)
                bursts = kleinberg(tsClean,timeRange=dataTimeRangeInSeconds,probesInUnit=numProbesInUnit)
                if burstDetectionPlot:
                    plotter.plotBursts(bursts,'figures/connectionBursts_'+str(key))

                burstsDict={}
                for brt in bursts:
                    q=brt[0]
                    qstart=brt[1]
                    qend=brt[2]
                    if q not in burstsDict.keys():
                        burstsDict[q]=[]
                    tmpDict={'start':float(qstart),'end':float(qend)}
                    burstsDict[q].append(tmpDict)

                thresholdedEvents=applyBurstThreshold(burstsDict,eventClean)
                print('con',key,len(thresholdedEvents),len(eventClean))

                if len(thresholdedEvents)>0:

                    intConCountryDict=groupByCountry(thresholdedEvents)
                    #print(intConCountryDict)
                    intConControllerDict=groupByController(thresholdedEvents)
                    intConProbeIDDict=groupByProbeID(thresholdedEvents)
                    intConASNDict=groupByASN(thresholdedEvents)
                    with closing(open('data/ne/choroConData.txt','w')) as fp:
                        print("CC,DISCON",file=fp)
                        for k,v in intConCountryDict.items():
                            probesOwned=len(probeInfo.countryToProbeIDDict[k])
                            probesInEvent=v
                            #Some probes could be added after the probe enrichment data was grabbed
                            if probesInEvent > probesOwned:
                                #print(k,v,probesOwned)
                                normalizedVal=1
                            else:
                                normalizedVal=float(probesInEvent)/probesOwned
                            print("{0},{1}".format(k,normalizedVal),file=fp)
                        #Hack to make sure data has atleast 2 elements
                        if len(intConCountryDict)==1:
                            if 'MC' not in intConCountryDict.keys():
                                print("{0},{1}".format('MC',0),file=fp)
                            elif 'VA' not in intConCountryDict.keys():
                                print("{0},{1}".format('VA',0),file=fp)
                        elif len(intConCountryDict)==0:
                                print("{0},{1}".format('MC',0),file=fp)
                                print("{0},{1}".format('VA',0),file=fp)

                    plotter.lock.acquire()
                    try:
                        if groupByCountryPlot and not countryKey:
                            plotter.plotDict(intConCountryDict,'figures/connectionsByCountry_'+str(key))
                        if groupByASNPlot and not asnKey:
                            plotter.plotDict(intConASNDict,'figures/connectionsByASN_'+str(key))
                        if groupByControllerPlot:
                            plotter.plotDict(intConControllerDict,'figures/connectionsByController_'+str(key))
                        if groupByProbeIDPlot:
                            plotter.plotDict(intConProbeIDDict,'figures/connectionsByProbeID_'+str(key))
                        if choroplethPlot:
                            #if len(intConCountryDict) > 1:
                            plotChoropleth('data/ne/choroConData.txt','figures/connectionChoroPlot_'+str(key)+'_'+plotter.suffix+'.png',plotter.getFigNum())
                        copyToServerFunc('con')
                        intConControllerDict.clear()
                        intConProbeIDDict.clear()
                        intConASNDict.clear()
                        intConCountryDict.clear()
                    except:
                        traceback.print_exc()
                    finally:
                        plotter.lock.release()
            for iter in range(0,itr2):
                dataQueueConnect.task_done()

def plotThings(type,key='All'):
    if type=='dis':
        if groupByCountryPlot:
            plotter.plotDict(intDisCountryDict,'figures/disconnectionsByCountry_'+str(key))
        if groupByASNPlot:
            plotter.plotDict(intDisASNDict,'figures/disconnectionsByASN_'+str(key))
        if groupByControllerPlot:
            plotter.plotDict(intDisControllerDict,'figures/disconnectionsByController_'+str(key))
        if groupByProbeIDPlot:
            plotter.plotDict(intDisProbeIDDict,'figures/disconnectionsByProbeID_'+str(key))
        if choroplethPlot:
            #if len(intDisCountryDict) > 1:
            plotChoropleth('data/ne/choroDisData.txt','figures/disconnectionChoroPlot_'+str(key)+'_'+plotter.suffix+'.png',plotter.getFigNum())
        copyToServerFunc('dis')
    if type=='con':
        if groupByCountryPlot:
            plotter.plotDict(intConCountryDict,'figures/connectionsByCountry_'+str(key))
        if groupByASNPlot:
            plotter.plotDict(intConASNDict,'figures/connectionsByASN_'+str(key))
        if groupByControllerPlot:
            plotter.plotDict(intConControllerDict,'figures/connectionsByController_'+str(key))
        if groupByProbeIDPlot:
            plotter.plotDict(intConProbeIDDict,'figures/connectionsByProbeID_'+str(key))
        if choroplethPlot:
            #if len(intConCountryDict) > 1:
            plotChoropleth('data/ne/choroConData.txt','figures/connectionChoroPlot_'+str(key)+'_'+plotter.suffix+'.png',plotter.getFigNum())
        copyToServerFunc('con')