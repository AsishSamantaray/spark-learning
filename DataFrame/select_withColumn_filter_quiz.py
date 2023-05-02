# In this program we'll learn how to use PySpark select, withCoulumn and filter/where.
# We'll be using StudentData.csv to practice select, withCoulumn and filter/where functions.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Create SparkSession Object as spark
spark = SparkSession.builder \
.appName("select, withCoulumn and filter Quiz") \
.master("local[*]") \
.getOrCreate()

# Read the CSV file
student_df = spark.read.format('csv') \
.options(inferSchema='true', header='true') \
.load('./StudentData.csv')

# Create a new column as total_marks and set value as 120 for all rows.
student_df = student_df.withColumn('total_marks', lit(120))

# Create a new column as average_mark to calculate average mark (marks / total_marks) / 100
student_df = student_df.withColumn('average_mark', lit((col('marks') / col('total_marks') * 100)))

# Find all the students who've achived more than 80% in OOP.
student_with_more_than_80_in_oop_df = student_df.filter((col('average_mark') > 80) & (col('course') == 'OOP'))

# Find all the students who've achived more than 60% in Cloud.
student_with_more_than_60_in_cloud_df = student_df.filter((col('average_mark') > 60) & (col('course') == 'Cloud'))

# Print name and marks column of all students of student_with_more_than_60_in_cloud_df.
student_df_with_names_marks_df = student_with_more_than_60_in_cloud_df.select('name', 'marks')


student_df_with_names_marks_df.show()


