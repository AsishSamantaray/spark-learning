# Word Count using DataFrame.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
.appName("Word Count") \
.master('local[*]') \
.getOrCreate()

word_df = spark.read.format('text').load('./WordData.txt')

word_count_df = word_df.withColumnRenamed('value', 'word').groupBy('word').agg(count('word').alias('word_count'))

word_count_df.show()