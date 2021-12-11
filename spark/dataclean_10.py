import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
#DOB NOW: Build Elevator Permit Applications
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.kfp4-dz4h.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[0] != "Job Filling Number")
data = data_full.filter(lambda x: x[9] != "") # make sure the elevator has a permit
data = data.filter(lambda x: x[22] != "") # make sure the the elevator is inspected
data = data.filter(lambda x: x[49] != "" and x[49] != "N/A") # make sure the the elevator has an owner

data = data.map(lambda x: [x[11], x[12], x[13], x[49]]).sortByKey()

def repair(x):
    x[0] = x[0].replace('one','1')
    return x

data_repaired = data.map(repair)

data_result = data_repaired.map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]))

data_result.saveAsTextFile("data10.tsv")
sc.stop()
