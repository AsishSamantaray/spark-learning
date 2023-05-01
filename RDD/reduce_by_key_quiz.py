# Write a program to count words in the file in (word, count) format.

from pyspark import SparkConf, SparkContext

conf = SparkConf() \
.setAppName("reduce_by_key Quiz") \
.setMaster("local[*]")

sc = SparkContext.getOrCreate(conf=conf)

lines = sc.textFile('./reduce_by_key_quiz.txt')

# Word Count! (key, value)
word_count = lines \
.map(lambda line: line.split(" ")) \
.flatMap(lambda words: words) \
.map(lambda word: (word, 1)) \
.reduceByKey(lambda a, b: a + b)

print(word_count.collect())
