from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/Capitales/capitals_distances.txt")

def createPairs(ligne):
    parts = ligne.split(';')
    return [(parts[1], parts[3], int(parts[4])), (parts[3], parts[1], int(parts[4]))]

pairs = data.flatMap(createPairs)

def removeFar(pairs):
    return pairs[2] <= 1000

pairs_close = pairs.filter(removeFar)

pairs_count = pairs_close.map(lambda pair: (pair[0], 1))

city_counts = pairs_count.reduceByKey(lambda a, b: a + b)

for res in city_counts.collect():
    print(f"{res[0], res[1]}")


sc.stop()