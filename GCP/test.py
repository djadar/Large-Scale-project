import random
from pyspark import SparkContext
from pyspark.conf import SparkConf
#%matplotlib inline
import matplotlib.pyplot as plt
#matplotlib.pyplot.switch_backend('agg')
import pandas as pd
import numpy as np

def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
datapath = "../clusterdata-2011-2/machine_events/data"
#part-00000-of-00001.csv"
wholeFile = sc.textFile(datapath)

# We set the comumn names in a an array
firstLine = ["time",
"machine ID",
"event type",
"platform ID",
"CPUs",
"Memory"
]
entries = wholeFile.map(lambda x : x.split(','))
entries.cache()

column_index1=findCol(firstLine, "machine ID")
column_index2=findCol(firstLine, "CPUs")

machineIDs = entries.map(lambda x: x[column_index1])
CPUs = entries.map(lambda x: x[column_index2])

def apply_mean(x):
	if x=='':
		x='0'

	return x

result = CPUs.map(lambda x: apply_mean(x))
d =result.countByValue()
#1
print(d)
print("==============================================")


x = [random.normalvariate(0,1) for i in range(100)]
rdd = sc.parallelize(x)

# Data for plotting
'''t = np.arange(0.0, 2.0, 0.01)
s = 1 + np.sin(2 * np.pi * t)

#s= CPUs.countByValue()
fig, ax = plt.subplots()
ax.plot(t, s)

ax.set(xlabel='time (s)', ylabel='voltage (mV)',
       title='About as simple as it gets, folks')
ax.grid()

fig.savefig("test.png")
plt.show()'''
lists = sorted(d.items()) # sorted by key, return a list of tuples

x, y = zip(*lists) # unpack a list of pairs into two tuples

plt.bar(x,y)
plt.show()
