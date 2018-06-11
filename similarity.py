from pyspark import SparkContext


sc = SparkContext("local","Simple App")
textfile = sc.textFile("hdfs://118.25.191.190:9000/similarity/random1.dat")

counts = textfile.flatMap(lambda line: line.split("\n")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)  
counts.saveAsTextFile("output")