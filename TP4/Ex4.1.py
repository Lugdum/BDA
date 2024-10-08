from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/worldcitiespop.txt")

header = data.first()
data_no_head = data.filter(lambda ligne: ligne != header)

def removeNoPop(line):
    return len(line.split(',')[4]) != 0

with_pop = data_no_head.filter(removeNoPop)

for res in with_pop.collect():
    print(res)

sc.stop()