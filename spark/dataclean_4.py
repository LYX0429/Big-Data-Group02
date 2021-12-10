import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.xi7c-iiu2.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[0] != "Borough")

data = data_full.map(lambda x: (x[0], x[4], x[5], x[6], x[7])).sortByKey()
#data = data.filter(lambda x: x[0] != "CONFIDENTIAL")
data = data.toDF().dropna(how='any')
data = data.rdd
data_result = data.map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]))

data_result.saveAsTextFile("data4.tsv")
sc.stop()
