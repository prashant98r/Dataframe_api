# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import datediff

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

tasks=[(1, "2024-07-01", "2024-07-10"), (2, "2024-08-01", "2024-08-15"), (3, "2024-09-01", "2024-09-05")]

tasks_df= spark.createDataFrame(tasks).toDF("task_id", "start_date","end_date")

tasks_df=tasks_df.withColumn("start_date",col("start_date").cast("date")) \
    .withColumn("end_date",col("end_date").cast("date"))

tasks_with_duration_df= (tasks_df.withColumn(
    "task_duration"
    ,when(datediff(col("end_date"),col("start_date"))<7,"Short")
    .when(datediff(col("end_date"),col("start_date")).between(7,14),"Medium")
    .otherwise("Long"))
)
tasks_with_duration_df.show()