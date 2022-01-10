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
		.setMaster("local[1]")
        .set("spark.driver.cores","6")
		
)
# start spark with 1 worker thread
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
datapath = "gs://bucket-large-scale-project/clusterdata-2011-2/machine_events"
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



datapath2 = "gs://bucket-large-scale-project/clusterdata-2011-2/tasks_events"
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
print(result)
print("processing time is: ", timedelta(seconds=end - start))
print("==============================================")

start = timer()
column_index22=findCol(firstLine2, "scheduling class")
print("{} corresponds to column {}".format("scheduling class", column_index22))


task_scheduling = entries2.map(lambda x: x[column_index22])

print(task_scheduling.countByValue())
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
print("processing time is: ", timedelta(seconds=end - start))
print("The percentage of ejected task being of low priority is {}%".format(round(percentage,3)))
print("\n Yes, tasks with low priority have a higher probability of being evicted.")
print("==============================================")

start = timer()
column_index25=findCol(firstLine2, "machine ID")
print("{} corresponds to column {}".format("machine ID", column_index25))


job_machine = entries2.map(lambda x: (x[column_index21],x[column_index25])).groupByKey().map(lambda x: (x[0], list(x[1])))
#.mapValues(list)
job_machine = job_machine.map(lambda x: (x[0],clean(x[1])))
end = timer()
print("processing time is: ", timedelta(seconds=end - start))

totalEnd = timer()

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


print("No, so we have a distributed system")
print("==============================================")
print ("total data processing time is: ", timedelta(seconds= totalEnd - totalStart))
print("==============================================")










'''


# Display the 5 first nationalities
print("A few examples of nationalities:")
for elem in nationalities.sortBy(lambda x: x).take(5):
	print(elem)

# 2- Print the total number of observations
print("2- Total number of observations is {}".format(entries.count()))

# 3- Count the number of years over which observations have been made

column_index = findCol(firstLine, "Year")
print("{} corresponds to column {}".format("Year", column_index))

years = entries.map(lambda x: x[column_index])

print("3- Number of years over which observations have been made is {}".format(years.count()))

#4- Display the oldest and the newest year of observation

year_observation = years.groupByKey()

# prevent the program from terminating immediatly
input("Press Enter to continue...")
'''
