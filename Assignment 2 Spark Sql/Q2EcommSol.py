import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, max, min, count

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.ui.port", "4040")
         .appName("spark-sql-student-analysis")
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

df1 = spark.createDataFrame(students).toDF("student_id", "name", "score", "subject")
df1.show()

df1.createOrReplaceTempView("students")

spark.sql("""
            SELECT 
            student_id,
            name,
            score,
            subject,
                CASE
                    WHEN score >= 90 THEN 'A'
                    WHEN score >= 80 AND score <90 THEN 'A'
                    WHEN score >= 70 AND score < 80 THEN 'C'
                    WHEN score >= 60 AND score < 70 THEN 'D'
                    ELSE 'F'
                END as grade
            FROM students


""").createOrReplaceTempView("students_with_grades")

spark.sql("""
        SELECT 
        subject,
        avg(score) as avg_score_per_subject
        FROM students_with_grades
        GROUP BY subject
""").show()

spark.sql("""
        SELECT 
        subject,
        MAX(score) as max_score_per_subject,
        MIN(score) as min_score_per_subject
        FROM students_with_grades
        GROUP BY subject
""").show()

spark.sql("""
        SELECT 
        subject,
        grade,
        COUNT(student_id) as student_count
        FROM students_with_grades
        GROUP BY subject,grade
""").show()