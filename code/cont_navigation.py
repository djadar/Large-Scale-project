from os import replace
import sys
from pyspark import SparkContext
import time

# Finds out the index of "name" in the array firstLine 
# returns -1 if it cannot find it
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
datapath1 = "../clusterdata-2011-2/task_usage/data.csv"
#part-00000-of-00001.csv"
wholeFile1 = sc.textFile(datapath1)

# We set the comumn names in a an array
firstLine1 = ["time",
"start time",
"end time",
"job ID",
"task index",
"machine ID",
"CPU rate",
"canonical memory usage",
"assigned memory usage",
"unmapped page cache",
"total page cache",
"maximum memory usage",
"disk I/O time",
"local disk space usage",
"maximum CPU rate",
"maximum disk IO time",
"cycles per instruction",
"memory accesses per instruction",
"sample portion",
"aggregation type",
"sampled CPU usage"
]
# filter out the first line from the initial RDD
#entries = wholeFile.filter(lambda x: not ("RecID" in x))

# split each line into an array of items
entries1 = wholeFile1.map(lambda x : x.split(','))

# keep the RDD in memory
entries1.cache()
column_index11=findCol(firstLine1, "job ID")
print("{} corresponds to column {}".format("job ID", column_index11))
column_index12=findCol(firstLine1, "task index")
print("{} corresponds to column {}".format("task index", column_index12))
column_index13=findCol(firstLine1, "CPU rate")
print("{} corresponds to column {}".format("CPU rate", column_index13))

CPU_resource_consumed = entries1.map(lambda x: ((x[column_index11],x[column_index12]),x[column_index13]))
#print(CPU_resource_consumed.collect())
def merge(list):
	return (list[0][0]+"-"+list[0][1], list[1])

CPU_resource_consumed = CPU_resource_consumed.map(lambda x : merge(x)).distinct()
('6183750753-60', '0.125')
#print(CPU_resource_consumed.collect())
##### Create an RDD that contains all machine ID observed in the
##### different entries

datapath2 = "../clusterdata-2011-2/task_events/data"
#part-00000-of-00001.csv"
wholeFile2 = sc.textFile(datapath2)

firstLine2 = [
"time",
"missing info",
"job ID",
"task index",
"machine ID",
"event type",
"user",
"scheduling class",
"priority",
"CPU request",
"memory request",
"disk space request",
"different machines restriction"]

entries2 = wholeFile2.map(lambda x : x.split(','))

# keep the RDD in memory
entries2.cache()
column_index21=findCol(firstLine2, "job ID")
print("{} corresponds to column {}".format("job ID", column_index21))
column_index22=findCol(firstLine2, "task index")
print("{} corresponds to column {}".format("task index", column_index22))
column_index23=findCol(firstLine2, "CPU request")
print("{} corresponds to column {}".format("CPU request", column_index23))

CPU_resource_requested = entries2.map(lambda x: ((x[column_index21],x[column_index22]),x[column_index23]))
##### Create an RDD that contains all machine ID observed in the
##### different entries

# Information about the machine ID is provided in the column named
# "machine ID"
CPU_resource_requested = CPU_resource_requested.map(lambda x : merge(x)).distinct()

CPU_ressource_join = CPU_resource_requested.join(CPU_resource_consumed).collect()


threshold = 0.05
def check(list):
	if float(list[0]) > threshold and float(list[1]) > threshold :
		return True

CPU_ressource_join = sc.parallelize(CPU_ressource_join).filter(lambda x: check(x[1]))
print(CPU_ressource_join.collect())
