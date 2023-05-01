# Write a program to print length of each words in the file.

from pyspark import SparkConf, SparkContext

conf = SparkConf() \
.set('spark.app.name', 'Map Quiz') \
.set('spark.master', 'local[*]')

sc = SparkContext.getOrCreate(conf=conf)

lines = sc.textFile("./map_quiz.txt")

word_count = lines.map(lambda line: line.split(" ")).map(lambda words: [len(word) for word in words])

print(word_count.collect())
