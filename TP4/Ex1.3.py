from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP3/bda_datasets-main/Livre/LesMiserables_T1.txt")

words = data.flatMap(lambda line: line.split())
histo = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for word, count in histo.collect():
    print(f"{word}: {count}")

sc.stop()