import matplotlib.pyplot as plt
import csv
import time

plt.ion()
plt.plot([], 'c',label='Disconnections',lw=3)
plt.plot([], 'g',label='Connections',lw=3)
plt.legend()
while True:
    abrListD=[]
    with open('discoResultsDisconnections.txt', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='|')
        for row in reader:
            abrListD.append(float(row[1]))

    abrListC=[]
    with open('discoResultsConnections.txt', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='|')
        for row in reader:
            abrListC.append(float(row[1]))

    plt.plot(abrListD, 'c',label='Disconnections',lw=3)
    plt.plot(abrListC, 'g',label='Connections',lw=3)
    minVal=min(min(abrListD),min(abrListC))
    maxVal=max(max(abrListD),max(abrListC))
    #plt.ylim(minVal-0.05,maxVal+0.05)
    plt.pause(1)
