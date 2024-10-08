from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)
data= sc.textFile("file:///home/victor/Epita/ING2/BDA/TP4/Capitales/capitals_distances.txt")

def createPairs(ligne):
    parts = ligne.split(';')
    return [(parts[1], (parts[3], int(parts[4]))), (parts[3], (parts[1], int(parts[4])))]

paires = data.flatMap(createPairs)

def findMinMax(a, b):
    if not isinstance(a[0], tuple):
        a = (a, a)
    if not isinstance(b[0], tuple):
        b = (b, b)
    
    minPair = min(a[0], b[0], key=lambda x: x[1])
    maxPair = max(a[1], b[1], key=lambda x: x[1])
    return (minPair, maxPair)

results = paires.combineByKey(lambda valeur: (valeur, valeur), lambda x, valeur: findMinMax(x, valeur), lambda x, y: findMinMax(x, y))

def formatterResultat(resultat):
    ville, ((vmin, dmin), (vmax, dmax)) = resultat
    return f"{ville}, vmin: {vmin} : {dmin}, vmax: {vmax} : {dmax}"

resultatsFinaux = results.map(formatterResultat)
resultatsFinaux.foreach(print)

sc.stop()