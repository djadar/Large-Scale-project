from os import replace
import sys
from pyspark import SparkContext
import time

from operator import add
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
firstLine1 = [
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
column_index13=findCol(firstLine1, "maximum CPU rate")
print("{} corresponds to column {}".format("maximum CPU rate", column_index13))

CPU_resource_consumed = entries1.map(lambda x: ((x[column_index11],x[column_index12]),x[column_index13])).filter(lambda x: x[1]!='')

def merge2(list):
	return (list[0][1]+"-"+list[0][0], list[1])

#CPU_resource_consumed = CPU_resource_consumed.map(lambda x : merge2(x)).distinct()
#print(CPU_resource_consumed.take(5))

#print(CPU_resource_consumed.collect())
##### Create an RDD that contains all machine ID observed in the
##### different entries

def merge(list):
	return (list[0][0]+"-"+list[0][1], list[1])

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

CPU_resource_requested = entries2.map(lambda x: ((x[column_index21], x[column_index22]),x[column_index23])).filter(lambda x: x[1] !='')
##### Create an RDD that contains all machine ID observed in the
##### different entries

# Information about the machine ID is provided in the column named
# "machine ID"
#DECOMMENT

#CPU_resource_requested = CPU_resource_requested.map(lambda x : merge(x)).distinct()
#print(CPU_resource_requested.take(5))
#DECOMMENT
CPU_ressource_join = CPU_resource_requested.join(CPU_resource_consumed)
#CPU_ressource_join = CPU_resource_requested.union(CPU_resource_consumed).reduceByKey(lambda a, b:(a,b))


threshold = 0.05
def check(list):
	#if float(list[0]) > threshold and float(list[1]) > threshold :
	if list[0] =='' :
		list = (0,list[1])
	if list[1] =='' :
		list = (list[0],0)
	#if float(list[1]) > (threshold * float(list[0])) :
	if (float(list[1]) >= threshold ) and (float(list[0]) >= threshold):
		return True
#6
#DECOMMENT
#CPU_ressource_join2 = sc.parallelize(CPU_ressource_join.collect()).filter(lambda x: check(x[1]))
#DECOMMENT
#percentage = (CPU_ressource_join2.count() * 100) / CPU_ressource_join.count()
#print("The percentage of tasks that request the more resources and consume the more resources is {}%".format(round(percentage,3)))
#print("\n Yes, the tasks that request the more resources the one that consume the more resources.")

#.collect())

print("==============================================")

datapath4 = "../clusterdata-2011-2/machine_events/data.csv"
#part-00000-of-00001.csv"
wholeFile4 = sc.textFile(datapath4)

firstLine4 = [
"time",
"machine ID",
"event type",
"platform ID",
"CPUs",
"Memory"]


entries4 = wholeFile4.map(lambda x : x.split(','))


column_index41=findCol(firstLine4, "machine ID")
print("{} corresponds to column {}".format("machine ID", column_index41))
column_index42=findCol(firstLine4, "event type")
print("{} corresponds to column {}".format("event type", column_index42))
column_index43=findCol(firstLine4, "CPUs")
print("{} corresponds to column {}".format("CPUs", column_index43))
#DECOMMENT
task_event_on_job = entries4.map(lambda x: (x[column_index41],x[column_index42],x[column_index43])).filter(lambda x: x[0]!='' and x[1]!='' and x[2]!='' )
task_event_on_job = task_event_on_job.filter(lambda x: float(x[2])>=0.5)
#print(task_event_on_job.collect())
percentage = (task_event_on_job.filter(lambda x: x[1]=='2') .count() * 100) / task_event_on_job.count()
print("The percentage of peaks of high resource consumption on some machines and task eviction events is {}%".format(round(percentage,3)))
print("\n Yes, twe observe correlations between peaks of high resource consumption on some machines and task eviction events.")


#machine_resource_consumption = entries1.map(lambda x: (x[column_index14],x[column_index13])).filter(lambda x: x[0]!='' and x[1]!='')
#print(machine_resource_consumption.collect())

#DECOMMENT
#CPU_resource_consumed_and_task_event = machine_resource_consumption.join(task_event_on_job)
#machine_resource_consumption.union(task_event_on_job).reduceByKey(lambda a, b:(a,b))
#machine_resource_consumption.join(task_event_on_job)
#print(CPU_resource_consumed.collect())
#DECOMMENT
#print(CPU_resource_consumed_and_task_event)


print("==============================================")
'''
If the ​
different-machine constraint ​
field is present, and true, it indicates that a task must be
scheduled to execute on a different machine than any other currently running task in the
job. It is a special type of constraint.'''

'''column_index25=findCol(firstLine2, "different machines restriction")
print("{} corresponds to column {}".format("different machines restriction", column_index25))

machine_constrainst_event = entries2.map(lambda x: ((x[column_index24], x[column_index25]))).distinct()
#print(machine_constrainst_event.collect())
datapath3 = "../clusterdata-2011-2/task_events/data"
#part-00000-of-00001.csv"
wholeFile3 = sc.textFile(datapath2)

firstLine3 = [
"time",
"job ID",
"task index",
"comparison operator",
"attribute name",
"attribute value"]

entries3 = wholeFile3.map(lambda x : x.split(','))

# keep the RDD in memory
entries3.cache()'''

