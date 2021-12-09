import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.hg8x-zxpr.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[0] != "Project ID")

data = data_full.map(lambda x: (x[1], x[5], x[6], x[8])).sortByKey()
data = data.filter(lambda x: x[0] != "CONFIDENTIAL")
data_result = data.map(lambda x: str(x[0]) + "\t" + str(x[1]) + " " + str(x[2]) + "\t" + str(x[3]))

data_result.saveAsTextFile("data1.tsv")
sc.stop()
