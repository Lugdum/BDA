from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/worldcitiespop.txt")

header = data.first()
data_no_head = data.filter(lambda ligne: ligne != header)

def removeNoPop(line):
    return len(line.split(',')[4]) != 0

with_pop = data_no_head.filter(removeNoPop)
pop = with_pop.map(lambda line: int(line.split(',')[4])).collect()

min_pop = min(pop)
max_pop = max(pop)
average_pop = sum(pop)/len(pop)
sum_pop = sum(pop)

print("Min pop: ", min_pop)
print("Max pop: ", max_pop)
print("Average pop: ", round(average_pop))
print("Sum pop: ", sum_pop)

sc.stop()