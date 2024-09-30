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
         .appName("spark-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Sample Data
employees = [(1, "2024-01-10", 8, "Good performance"),
             (2, "2024-01-15", 9, "Excellent work!"),
             (3, "2024-02-20", 6, "Needs improvement"),
             (4, "2024-02-25", 7, "Good effort"),
             (5, "2024-03-05", 10, "Outstanding!"),
             (6, "2024-03-12", 5, "Needs improvement")]

df1 = spark.createDataFrame(employees).toDF("employee_id", "review_date", "performance_score", "review_text")
df1.show()

df1.createOrReplaceTempView("review")

spark.sql("""
    SELECT 
        employee_id,
        review_date,
        avg(performance_score) as average,
        review_text,
        CASE
            WHEN performance_score >= 9 THEN 'Excellent'
            WHEN performance_score BETWEEN 7 AND 8 THEN 'Good'
            ELSE 'Needs Improvement'
        END AS performance_category
    FROM review
    WHERE Lower(review_text) like '%excellent%'
    GROUP BY employee_id,
        review_date,
        performance_score,
        review_text  
""").show()
