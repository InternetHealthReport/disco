import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import traceback
from geonamescache import GeonamesCache
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
from mpl_toolkits.basemap import Basemap


def plotChoropleth(filename,imgfile,figNum):
    shapefile = 'data/ne/ne_10m_admin_0_countries'
    cols = ['CC', 'DISCON']
    num_colors = 20
    gc = GeonamesCache()
    iso_codes = list(gc.get_dataset_by_key(gc.get_countries(), 'iso').keys())
    df = pd.read_csv(filename, skiprows=0, usecols=cols)
    df.set_index('CC', inplace=True)
    df = df.ix[iso_codes].dropna() # Filter out non-countries and missing values.
    values = df['DISCON']
    cm = plt.get_cmap('Reds')
    scheme = [cm(float(i) / num_colors) for i in range(num_colors)]
    #bins = np.linspace(values.min(), values.max(), num_colors)
    bins = np.linspace(0, 1, num_colors)
    df['bin'] = np.digitize(values, bins) - 1
    df.sort_values('bin', ascending=False)#.head(10)

    #print(df)

    mpl.style.use('seaborn-pastel')
    print('Plotting Figure {0}: {1}'.format(figNum,imgfile))
    fig = plt.figure(figNum,figsize=(22, 12))

    ax = fig.add_subplot(111, axisbg='w', frame_on=False)
    #plt.title('Disco Choropleth', fontsize=20)#, y=.95)

    m = Basemap(lon_0=0, projection='robin')
    m.drawmapboundary(color='w')

    m.readshapefile(shapefile, 'units', color='#444444', linewidth=.2)
    for info, shape in zip(m.units_info, m.units):
        #iso = info['ADM0_A3']
        iso = info['ISO_A2']
        #print(iso)
        try:
            if iso not in df.index:
                color = '#dddddd'
            else:
                color = scheme[int(df.ix[iso]['bin'])]
        except TypeError:
            print(iso)
            traceback.print_exc()

        patches = [Polygon(np.array(shape), True)]
        pc = PatchCollection(patches)
        pc.set_facecolor(color)
        ax.add_collection(pc)

    # Cover up Antarctica so legend can be placed over it.
    ax.axhspan(0, 1000 * 1800, facecolor='w', edgecolor='w', zorder=2)

    # Draw color legend.
    ax_legend = fig.add_axes([0.35, 0.14, 0.3, 0.03], zorder=3)
    cmap = mpl.colors.ListedColormap(scheme)
    cb = mpl.colorbar.ColorbarBase(ax_legend, cmap=cmap, ticks=bins, boundaries=bins, orientation='horizontal')
    cb.ax.set_xticklabels([str(round(i, 2)) for i in bins],rotation='80')

    # Set the map footer.
    #plt.annotate(descripton, xy=(-.8, -3.2), size=14, xycoords='axes fraction')

    plt.savefig(imgfile, bbox_inches='tight', pad_inches=.2)
    #try:
    #    command=('scp {0} chekov.netsec.colostate.edu:public_html/iij/{1}/'.format(imgfile,imgfile.split('_')[1].split('.')[0]))
    #    os.system(command)
    #except:
    #    pass

if __name__=="__main__":
    filename = 'data/ne/choroData.txt'
    imgfile = 'figures/{0}.png'.format('choroPlot')
    figNum=1
    plotChoropleth(filename,imgfile,figNum)
