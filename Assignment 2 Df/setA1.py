import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, max, min, count

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] ="C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"]="C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.ui.port", "4040")
         .appName("spark-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Sample Data
students = [
    (1, "Alice", 92, "Math"),
    (2, "Bob", 85, "Math"),
    (3, "Carol", 77, "Science"),
    (4, "Dave", 65, "Science"),
    (5, "Eve", 50, "Math"),
    (6, "Frank", 82, "Science")
]

# Create DataFrame
students_df = spark.createDataFrame(students).toDF("student_id", "name", "score", "subject")

# Fix: Adding parentheses around the conditions
students_df_with_grades = students_df.select(
    col("student_id"),
    col("name"),
    col("score"),
    col("subject"),
    when(col("score") >= 90, "A")
    .when((col("score") >= 80) & (col("score") < 90), "B")
    .when((col("score") >= 70) & (col("score") < 80), "C")
    .when((col("score") >= 60) & (col("score") < 70), "D")
    .otherwise("F").alias("grade")
)

# Show the resulting DataFrame with grades
students_df_with_grades.show()

# Average score per subject
avg_score_per_subject = students_df.groupby("subject").agg(avg("score").alias("avg_score"))
avg_score_per_subject.show()

# Maximum score per subject
max_score_per_subj = students_df.groupby("subject").agg(max("score").alias("max_score"))
max_score_per_subj.show()

# Minimum score per subject
min_score_per_subj = students_df.groupby("subject").agg(min("score").alias("min_score"))
min_score_per_subj.show()

# Count of students by grade and subject
count_of_stud = students_df_with_grades.groupby("grade", "subject").agg(count("student_id").alias("count_of_stud"))
count_of_stud.show()
