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


data=[100.0,100.0,100.0,100.0,66.6666666667,0.0,25.0,0.0,100.0,100.0,0.0,0.0,0.0,0.0,0.0,0.0,88.8888888889,\
      0.0,100.0,0.0,100.0,100.0,100.0,100.0,100.0,100.0,8.33333333333,94.7368421053,80.0,12.5,100.0,18.1818181818,\
      0.0,0.0,100.0,0.0,75.0,0.0,100.0,0.0,100.0,75.0,7.69230769231,81.25,75.0,40.0,0.0,100.0,100.0,100.0,70.0,0.0,\
      90.9090909091,100.0,26.6666666667,100.0,0.0,100.0,100.0,66.6666666667,100.0,50.0,100.0,50.0,100.0,93.75,0.0]

ecdf(data,'hitsWithTrinocular.png')