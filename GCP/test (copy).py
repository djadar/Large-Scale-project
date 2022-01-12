import random
from pyspark import SparkContext
from pyspark.conf import SparkConf
#%matplotlib inline
import matplotlib.pyplot as plt
#matplotlib.pyplot.switch_backend('agg')
import pandas as pd
import numpy as np


conf = (SparkConf()
		.setMaster("local[*]")
        .set("spark.driver.cores","6")
		
)
# start spark with 1 worker thread
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# create an RDD of 100 random numbers
x = [random.normalvariate(0,1) for i in range(100)]
rdd = sc.parallelize(x)

# Data for plotting
t = np.arange(0.0, 2.0, 0.01)
s = 1 + np.sin(2 * np.pi * t)

fig, ax = plt.subplots()
ax.plot(t, s)

ax.set(xlabel='time (s)', ylabel='voltage (mV)',
       title='About as simple as it gets, folks')
ax.grid()

fig.savefig("test.png")
plt.show()
