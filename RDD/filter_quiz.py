# Write a program to print words that doesn't starts with a and c.

from pyspark import SparkConf, SparkContext

conf = SparkConf() \
.set('spark.app.name', 'Filter Quiz') \
.setMaster('local[*]')

sc = SparkContext.getOrCreate(conf=conf)

lines = sc.textFile('./filter_quiz.txt')

word_without_a_or_c = lines.map(lambda line: line.split(" ")).flatMap(lambda words: words).filter(lambda word: (not word.startswith('a')) and (not word.startswith('c')))

print(word_without_a_or_c.collect())