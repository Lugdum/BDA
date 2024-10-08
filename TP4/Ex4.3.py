from pyspark import SparkConf, SparkContext
import math
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/worldcitiespop.txt")

header = data.first()
data_no_head = data.filter(lambda ligne: ligne != header)

def removeNoPop(line):
    return len(line.split(',')[4]) != 0

with_pop = data_no_head.filter(removeNoPop)
pop = with_pop.map(lambda line: (line.split(',')[1], int(line.split(',')[4])))

def get_population_class(population):
    return int(math.log10(population) // 1)

histogram = pop.map(lambda x: (get_population_class(x[1]), 1)).reduceByKey(lambda a, b: a + b)

for class_, count in histogram.collect():
    print(f"Class {class_}: {count} cities")

sc.stop()