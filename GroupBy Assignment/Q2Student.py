import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] ="C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"]="C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.eventLog.enabled", "false")
         .appName("spark-orders-program")
         .master("local[*]")
         .getOrCreate())


log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.OFF)
log4jLogger.LogManager.getLogger("akka").setLevel(log4jLogger.Level.OFF)


# Sample Data
student_data = [
    ("Alice", "Math", 80),
    ("Bob", "Math", 90),
    ("Alice", "Science", 70),
    ("Bob", "Science", 85),
    ("Alice", "English", 75),
    ("Bob", "English", 95)
]

# Create DataFrame
student_df = spark.createDataFrame(student_data).toDF("student_name", "subject", "score")




subject_avg_df = student_df.groupBy("subject").agg(
    avg("score").alias("average_score")
)
subject_avg_df.show()


student_max_score_df = student_df.groupBy("student_name").agg(
    max("score").alias("maximum_score")
)
student_max_score_df.show()


student_df.createOrReplaceTempView("student_data")

spark.sql("""
            SELECT 
            subject,
            Avg(score) as average_score
            FROM student_data
            GROUP BY subject
""").show()

spark.sql("""
    SELECT 
        student_name, 
        MAX(Score) AS Max_Score
    FROM student_data
    GROUP BY student_name
""").show()