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

orders=[(1, 5, 100), (2, 10, 150), (3, 20, 300)]

orders_df= spark.createDataFrame(orders).toDF("order_id", "quantity","total_price")

orders_with_type_df= (orders_df.withColumn(
    "order_type"
    ,when((col("quantity")<10) & (col("total_price")<200),"Small & Cheap")
    .when((col("quantity")>=10) & (col("total_price")<200),"Bulk & Discounted")
    .otherwise( "Premium Order" ))
)
orders_with_type_df.show()