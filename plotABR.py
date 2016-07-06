import matplotlib.pyplot as plt
import csv

abrList=[]
with open('discoResults.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter='|')
    for row in reader:
        abrList.append(float(row[1]))

plt.plot(abrList, 'c')
plt.ylim(0,max(abrList)+0.5)
plt.show()