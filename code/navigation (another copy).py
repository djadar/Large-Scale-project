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
wholeFile = sc.textFile("./data/CLIWOC15.csv")

# The first line of the file defines the name of each column in the cvs file
# We store it as an array in the driver program
firstLine = wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"','').split(',')

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not ("RecID" in x))

# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

##### Create an RDD that contains all nationalities observed in the
##### different entries

# Information about the nationality is provided in the column named
# "Nationality"

# First find the index of the column corresponding to the "Nationality"
column_index=findCol(firstLine, "Nationality")
print("{} corresponds to column {}".format("Nationality", column_index))

# Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates 
nationalities = entries.map(lambda x: x[column_index])

def clean(s):
	#print(s)
	return s.replace(" ","")
	

# 1- Clean the entries
nationalities = nationalities.map(lambda x: clean(x)).distinct()

'''
for elem in nationalities.take(10):
	print(elem)

nationalities_item = nationalities.map((a) => a.replace(' ','') )
for elem in nationalities_item:
	elem.replace(' ','')
	
nationalities = nationalities_item
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
