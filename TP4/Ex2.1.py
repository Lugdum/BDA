from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/arbres.csv")

header = data.first()
dataSansEntete = data.filter(lambda ligne: ligne != header)

print(f"Number of lignes: {data.count()}")

sc.stop()