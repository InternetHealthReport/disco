from matplotlib import pyplot as plt
import numpy as np
def ecdf(data, outfileName, xlabel='', ylabel='',titleInfo=''):
    print('Plotting Figure {0}: {1}'.format(1, outfileName))
    fig = plt.figure(1, figsize=(10, 8))
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(titleInfo)
    sorted = np.sort(data)
    yvals = np.arange(len(sorted)) / float(len(sorted))
    plt.plot(sorted, yvals)
    plt.grid()
    plt.autoscale()
    plt.savefig(outfileName)
    plt.close(fig)


data=[469,428,363,308,256,203,135,75,16,2,2,0,0,0]

ecdf(data,'timeWindow.png')