from pyspark import SparkContext


sc = SparkContext("local","Simple App")
textfile = sc.textFile("random2.dat")

counts = textfile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, [{len(word): 5}])).reduceByKey(lambda a, b: a+b)
print(type(counts))
counts.reduceByKey(lambda a, b: a + b)
print(counts.collect())

#counts.saveAsTextFile("output")