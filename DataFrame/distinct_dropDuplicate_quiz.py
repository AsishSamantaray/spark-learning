# In this program we'll learn about PySpark distinct() and dropDuplicates() functions.

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("PySpark distinct() and dropDuplicates() functions") \
.master("local[*]") \
.getOrCreate()

student_df = spark.read.format('csv') \
.options(inferSchema='true', header='True', path='./StudentData.csv') \
.load()

print(f'Student count Before :: {student_df.count()}')

# Show all unique rows for age, gender and course columns. (Using distinct())
student_df = student_df.select("age", "gender", "course").distinct()


# Show all unique rows for age, gender and course columns. (Using dropDistinct())
# student_df = student_df.dropDuplicates(["age", "gender", "course"])

print(f'Student count After :: {student_df.count()}')

student_df.show()