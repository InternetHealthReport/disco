import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import numpy as np
#data=[0.45,12.42,36.12,44.47,6.55]
data=[2,55,160,197,29]
fig, ax = plt.subplots()
fig.set_size_inches(5.5,3)

# Plot the bars
bars=ax.bar(np.arange(len(data)), data, align='center' ,color='0.4')
ax.set_xlabel('Traceroutes during outage')
ax.set_ylabel('#Outages')

# Show the 50% mark, which would indicate an equal
# number of tasks being completed by the robot and the
# human. There are 39 tasks total, so 50% is 19.5
#ax.hlines(19.5, -0.5, 5.5, linestyle='--', linewidth=1)

# Set a reasonable y-axis limit
ax.set_ylim(0, 50)
plt.grid()
plt.autoscale()
plt.tight_layout()
# Apply labels to the bars so you know which is which
ax.set_xticks(np.arange(len(data)))
ax.set_xticklabels(["100%\nComplete","1-50%\nInomplete","51-70%\nInomplete","71-100%\nInomplete","No\nTraceroute"],fontsize=9)#,rotation=45)
fig.savefig('tracerouteAnalysis.eps', format='eps', dpi=1000)