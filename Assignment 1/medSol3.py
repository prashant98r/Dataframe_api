# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

orders=[(1, "2024-07-01"), (2, "2024-12-01"), (3, "2024-05-01")]

orders_df= spark.createDataFrame(orders).toDF("order_id", "order_date")
orders_with_season= orders_df.withColumn(
    "season"
    ,when(month(col("order_date")).isin([6,7,8]),"Summer")
    .when(month(col("order_date")).isin([12,1,2]),"Winter")
    .otherwise("Other")
)
orders_with_season.show()