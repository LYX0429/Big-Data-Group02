import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.rbx6-tga4.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[0] != "Job Filing Number")

data = data_full.map(lambda x: [str(x[2]), x[3], x[30]])
data = data.filter(lambda x: 
                        x[0] != "" and
                        x[1] != "" and
                        x[2] != "")
                        
def repair(x):
    x[0] = x[0].replace('One','1')
    x[0] = x[0].replace('I','1')
    return x
    
data_repaired = data.map(repair).sortBy(lambda x:x[0]).sortBy(lambda x:x[1])
data_result = data_repaired.map(lambda x: str(x[0]) + " " + str(x[1]) + "\t" + str(x[2]))

data_result.saveAsTextFile("data6.tsv")
sc.stop()
