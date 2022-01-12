from os import replace
import sys
from pyspark import SparkContext
# import time
from timeit import default_timer as timer
from datetime import timedelta

from pyspark.conf import SparkConf


 
# Finds out the index of "name" in the array firstLine 
# returns -1 if it cannot find it
def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


def apply_mean(list):
	return list


#### Driver program
totalStart = timer()
conf = (SparkConf()
		.setMaster("local[*]")
        .set("spark.driver.cores","6")
		.set("spark.executor.memory", "5g")
		.set("spark.executor.cores", "100")
		#.set("spark.default.parallelism","6")
		
)
# start spark with 1 worker thread
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
datapath = "../clusterdata-2011-2/machine_events/data"
#part-00000-of-00001.csv"
wholeFile = sc.textFile(datapath,3)
# We set the comumn names in a an array
firstLine = ["time",
"machine ID",
"event type",
"platform ID",
"CPUs",
"Memory"
]
# filter out the first line from the initial RDD
#entries = wholeFile.filter(lambda x: not ("RecID" in x))

# split each line into an array of items
entries = wholeFile.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

##### Create an RDD that contains all machine ID observed in the
##### different entries

# Information about the machine ID is provided in the column named
# "machine ID"
print("==============================================")
print("number of partitions : ", wholeFile.getNumPartitions())

start = timer()

column_index1=findCol(firstLine, "machine ID")
print("{} corresponds to column {}".format("machine ID", column_index1))

column_index2=findCol(firstLine, "CPUs")
print("{} corresponds to column {}".format("CPUs", column_index2))

# Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates 
machineIDs = entries.map(lambda x: x[column_index1])
print("Le nombre de machines est {}".format(machineIDs.count()))
print("Le nombre de machines distincts est {}".format(machineIDs.distinct().count()))

CPUs = entries.map(lambda x: x[column_index2])
print("disrtibution of machines are as follows: ")
print("Le nombre de CPUs est {}".format(CPUs.count()))
print("Le nombre de CPUs distincts est {}".format(CPUs.distinct().count()))
M_y_temp = entries.map(lambda x: (x[column_index1],x[column_index2]))
result = M_y_temp.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list).map(lambda x: list(x))

start = timer()
result = result.map(lambda x: apply_mean(x))
#1
print(CPUs.countByValue())

end = timer()
print("processing time is: ", timedelta(seconds=end - start))
#for r in CPUs.countByValue().collect():
#	print(r)





datapath2 = "../clusterdata-2011-2/task_events/data"
#part-00000-of-00001.csv"
wholeFile2 = sc.textFile(datapath2,3)
# wholeFile2 = wholeFile2.repartition(10).glom().collect()
# wholeFile2 = sc.parallelize(wholeFile2)

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
print("==============================================")
print("number of partitions : ", wholeFile2.getNumPartitions())
# keep the RDD in memory
entries2.cache()

##### Create an RDD that contains all machine ID observed in the
##### different entries

# Information about the machine ID is provided in the column named
# "machine ID"
start = timer()
column_index21=findCol(firstLine2, "job ID")
print("{} corresponds to column {}".format("job ID", column_index21))


jobs = entries2.map(lambda x: x[column_index21])
#print("Le nombre de jobs est {}".format(jobs.countByValue()))
#2
result = sc.parallelize(jobs.countByValue().items()).map(lambda x : x[1]).mean()
end = timer()
print("average number of task composed in a job : ", result)
print("processing time is: ", timedelta(seconds=end - start))
print("==============================================")

start = timer()
column_index22=findCol(firstLine2, "scheduling class")
print("{} corresponds to column {}".format("scheduling class", column_index22))


task_scheduling = entries2.map(lambda x: x[column_index22])

print("distribution of jobs/tasks per scheduling class is: ", task_scheduling.countByValue())
end = timer()
print("processing time is: ", timedelta(seconds=end - start))

print("==============================================")

start = timer()
column_index23=findCol(firstLine2, "priority")
print("{} corresponds to column {}".format("priority", column_index23))

column_index24=findCol(firstLine2, "event type")
print("{} corresponds to column {}".format("event type", column_index24))

priority_event = entries2.map(lambda x: (x[column_index23],x[column_index24]))

priority_evicted = priority_event.filter(lambda x:x[1]=='2')

 #low_priority_event = priority_event.filter(lambda x:x[0]=='0')
#4
percentage = (priority_evicted.countByValue()[('0','2')] * 100) / priority_evicted.count()
end = timer()
print("The percentage of ejected task being of low priority is {}%".format(round(percentage,3)))
print("\n Yes, tasks with low priority have a higher probability of being evicted.")
print("processing time is: ", timedelta(seconds=end - start))
print("==============================================")

start = timer()
column_index25=findCol(firstLine2, "machine ID")
print("{} corresponds to column {}".format("machine ID", column_index25))


job_machine = entries2.map(lambda x: (x[column_index21],x[column_index25])).groupByKey().map(lambda x: (x[0], list(x[1])))
#.mapValues(list)
job_machine = job_machine.map(lambda x: (x[0],clean(x[1])))
end = timer()
# print("job per machine distribution: ", job_machine)
print("No, so we have a distributed system")
print("processing time is: ", timedelta(seconds=end - start))


def clean(list):
	count = 0
	for i in list :
		if i:
			count +=1
	return count

#print(job_machine.collect())
#print(job_machine.map(lambda x: clean(list(x[1])).collect()))

#5
#DECOMMENT
#print(job_machine.collect())


print("==============================================")






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
start = timer()
column_index11=findCol(firstLine1, "job ID")
print("{} corresponds to column {}".format("job ID", column_index11))
column_index12=findCol(firstLine1, "task index")
print("{} corresponds to column {}".format("task index", column_index12))
column_index13=findCol(firstLine1, "maximum CPU rate")
print("{} corresponds to column {}".format("maximum CPU rate", column_index13))
column_index14=findCol(firstLine1, "maximum memory usage")
print("{} corresponds to column {}".format("maximum Memory rate", column_index14))

CPU_resource_consumed = entries1.map(lambda x: ((x[column_index11],x[column_index12]),x[column_index13])).filter(lambda x: x[1]!='')
Memory_resource_consumed = entries1.map(lambda x: ((x[column_index11],x[column_index12]),x[column_index14])).filter(lambda x: x[1]!='')
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
column_index24=findCol(firstLine2, "memory request")
print("{} corresponds to column {}".format("Memory request", column_index24))

CPU_resource_requested = entries2.map(lambda x: ((x[column_index21], x[column_index22]),x[column_index23])).filter(lambda x: x[1] !='')
Memory_resource_requested = entries2.map(lambda x: ((x[column_index21], x[column_index22]),x[column_index24])).filter(lambda x: x[1] !='')
##### Create an RDD that contains all machine ID observed in the
##### different entries

# Information about the machine ID is provided in the column named
# "machine ID"
#DECOMMENT

#CPU_resource_requested = CPU_resource_requested.map(lambda x : merge(x)).distinct()
#print(CPU_resource_requested.take(5))
#DECOMMENT
CPU_ressource_join = CPU_resource_requested.join(CPU_resource_consumed)
Memory_resource_join = Memory_resource_requested.join(Memory_resource_consumed)
end = timer()
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
CPU_ressource_join2 = sc.parallelize(CPU_ressource_join.collect()).filter(lambda x: check(x[1]))
Memory_resource_join2 = sc.parallelize(Memory_resource_join.collect()).filter(lambda x: check(x[1]))
#DECOMMENT
percentage = (CPU_ressource_join2.count() * 100) / CPU_ressource_join.count()
percentage2 = (Memory_resource_join2.count() * 100) / Memory_resource_join.count()
print("The percentage of tasks that request the more CPU and consume the more CPU is {}%".format(round(percentage,3)))
print("The percentage of tasks that request the more memory and consume the more memory is {}%".format(round(percentage2,3)))
print("\n no, the tasks that request the more resources the one that consume the more resources.")

#.collect())
print("processing time is: ", timedelta(seconds=end - start))
print("==============================================")

# datapath4 = "../clusterdata-2011-2/machine_events/data"
# #part-00000-of-00001.csv"
# wholeFile4 = sc.textFile(datapath4)

# firstLine4 = [
# "time",
# "machine ID",
# "event type",
# "platform ID",
# "CPUs",
# "Memory"]


# entries4 = wholeFile4.map(lambda x : x.split(','))

start = timer()
column_index41=findCol(firstLine, "machine ID")
print("{} corresponds to column {}".format("machine ID", column_index41))
column_index42=findCol(firstLine, "event type")
print("{} corresponds to column {}".format("event type", column_index42))
column_index43=findCol(firstLine, "CPUs")
print("{} corresponds to column {}".format("CPUs", column_index43))
#DECOMMENT

task_event_on_job = entries.map(lambda x: (x[column_index41],x[column_index42],x[column_index43])).filter(lambda x: x[0]!='' and x[1]!='' and x[2]!='' )
task_event_on_job = task_event_on_job.filter(lambda x: float(x[2])>=0.5)


# task_event_on_job = entries4.map(lambda x: (x[column_index41],x[column_index42],x[column_index43])).filter(lambda x: x[0]!='' and x[1]!='' and x[2]!='' )
# task_event_on_job = task_event_on_job.filter(lambda x: float(x[2])>=0.5)
#print(task_event_on_job.collect())
percentage = (task_event_on_job.filter(lambda x: x[1]=='2') .count() * 100) / task_event_on_job.count()
end = timer()
print("The percentage of peaks of high resource consumption on some machines and task eviction events is {}%".format(round(percentage,3)))
print("\n Yes, twe observe correlations between peaks of high resource consumption on some machines and task eviction events.")
print("processing time is: ", timedelta(seconds=end - start))
totalEnd = timer()
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
print ("total data processing time is: ", timedelta(seconds= totalEnd - totalStart))
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