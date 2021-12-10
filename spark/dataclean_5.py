import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.8isn-pgv3.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[3] != "Borough")

data = data_full.map(lambda x: (x[1], x[2], x[19], x[20], x[21], x[22])).sortByKey()
data_result = data.map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]) + "\t" + str(x[5]))

data_result.saveAsTextFile("data5.tsv")
sc.stop()
