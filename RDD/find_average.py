# Write a program to find the average temperature of each months from average_quiz.csv file.

from pyspark import SparkConf, SparkContext

conf = SparkConf() \
.setAppName("Find Average temperature") \
.setMaster('local[*]')

sc = SparkContext.getOrCreate(conf=conf)

lines = sc.textFile("./average_quiz.csv")

average_temperature = lines.map(lambda line: (line.split(",")[0], (float(line.split(",")[2]), 1))) \
.reduceByKey(lambda a, b: (a[0] + b[0], a[0] + b[1])).map(lambda temp: (temp[0], round(temp[1][0] / temp[1][1], 2)))

print(average_temperature.collect())