from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/arbres.csv")

header = data.first()
dataSansEntete = data.filter(lambda ligne: ligne != header)

kinds = data.map(lambda line: (line.split(";")[2], 1))
nb_kinds = kinds.reduceByKey(lambda a, b: a + b)

print(nb_kinds.collect())

nb_kinds2 = kinds.countByKey()

for kind, count in nb_kinds2.items():
    print(f"{kind}: {count}")

sc.stop()