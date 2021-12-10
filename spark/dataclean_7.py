import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.3h2n-5cm9.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[0] != "ISN_DOB_BIS_VIOL")

data = data_full.map(lambda x: (x[7] + " " + x[8], x[5], x[6], x[15], x[16])).sortBy(lambda x: x[0])
data_result = data.map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]))

data_result.saveAsTextFile("data7.tsv")
sc.stop()
