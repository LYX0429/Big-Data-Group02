import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
#Inclusionary Housing Projects
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.jafx-rvrb.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[0] != "Project ID")
data_full = data_full.filter(lambda x: x[3] == "Completed")
data_full = data_full.filter(lambda x: x[6] != "0")
data_full = data_full.filter(lambda x: x[6] != "-2")
data = data_full.map(lambda x: (x[1], x[6])).sortByKey()
data_result = data.map(lambda x: str(x[0]) + "\t" + str(x[1]))
data_result.saveAsTextFile("data9.tsv")
sc.stop()