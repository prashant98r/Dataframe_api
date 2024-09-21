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

sales=[(1, 100), (2, 1500), (3, 300)]

sales_df= spark.createDataFrame(sales).toDF("sale_id", "amount")
sales_with_discount= sales_df.withColumn(
    "discount"
    ,when(col("amount")<200,0)
    .when((col("amount")>=200) & (col("amount")<=1000),10)
    .otherwise(20)
)
sales_with_discount.show()