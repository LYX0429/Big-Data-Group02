import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    data_open = sc.textFile(sys.argv[1], 1)
    data_open = data_open.mapPartitions(lambda x: reader(x))
    data_open = data_open.filter(lambda x: x[0] != "summons_number")

    data_open = data_open.map(lambda x: (x[5], (float(x[12]), 1))).sortByKey()
    data_result = data_open.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    data_result = data_result.map(lambda x: str(x[0]) + "\t" + "{:.2f}".format(x[1][0]) + ", " + "{:.2f}".format(x[1][0] / x[1][1]))

    data_result.saveAsTextFile("task3.out")
    sc.stop()
