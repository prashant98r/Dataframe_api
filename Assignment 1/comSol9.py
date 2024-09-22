# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import datediff, to_date

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

payments=[(1, "2024-07-15"), (2, "2024-12-25"), (3, "2024-11-01") ]

payments_df= spark.createDataFrame(payments).toDF("payment_id", "payment_date")
payments_df = payments_df.withColumn("payment_date", col("payment_date").cast("date"))

payments_with_column_df= (payments_df.withColumn(
    "quarter"
    ,when(month(col("payment_date")).between(1,3),"Q1")
    .when(month(col("payment_date")).between(4,6),"Q2")
    .when(month(col("payment_date")).between(7,9),"Q3")
    .otherwise("Q4"))
)
payments_with_column_df.show()