from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/arbres.csv")

letters = data.map(lambda line: len(line))
words = data.map(lambda line: len(line.split()))
lines = data.map(lambda line: 1)

nb_letters = letters.reduce(lambda a, b: a + b)
nb_words = words.reduce(lambda a, b: a + b)
nb_lines = lines.reduce(lambda a, b: a + b)

print(f"Number of lignes: {nb_lines}")
print(f"Number of mots: {nb_words}")
print(f"Number of lettres: {nb_letters}")

sc.stop()