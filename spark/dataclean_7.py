import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
fileName = "/user/CS-GY-6513/project_data/data-cityofnewyork-us.3h2n-5cm9.csv"
data_full = sc.textFile(fileName, 1)
data_full = data_full.mapPartitions(lambda x: reader(x))
data_full = data_full.filter(lambda x: x[0] != "ISN_DOB_BIS_VIOL")

data = data_full.map(lambda x: [str(x[8]), str(x[9]), x[6], x[7], x[16], x[17]])

data = data.filter(lambda x: 
                        x[0] != "" and
                        x[1] != "" and
                        x[2] != "" and
                        x[3] != "" and
                        x[4] != "" and
                        x[5] != "")

def repair(x):
    x[0] = x[0].replace(' ','')
    x[0] = x[0].replace('\xa0','')
    x[0] = x[0].replace("'",'')
    x[0] = x[0].replace('..4572','4572')
    x[0] = x[0].replace("\\",'')
    x[0] = x[0].replace("`",'')
    x[0] = x[0].replace("¦",'')
    x[0] = x[0].replace('+','')
    x[0] = x[0].replace('-','')
    
    x[1] = x[1].replace('  ',' ')
    x[1] = x[1].replace("'",'')
    x[1] = x[1].replace('`','')
    x[1] = x[1].replace('¦','')
    x[1] = x[1].replace('+','')
    x[1] = x[1].replace('-','')
    while len(x[0]) > 0 and x[0][0] == '0':
        x[0] = x[0][1:]
    while len(x[1]) > 0 and x[1][0] == '.':
        x[1] = x[1][1:]
    return x
    
data_repaired = data.map(repair)
data_repaired = data_repaired.filter(lambda x: x[0] != "").sortBy(lambda x:x[0]).sortBy(lambda x:x[1])

data_result = data_repaired.map(lambda x: str(x[0]) + " " + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]) + "\t" + str(x[5]))

data_result.saveAsTextFile("data7.tsv")
sc.stop()
