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
order_data = [
    ("Order1", "John", 100),
    ("Order2", "Alice", 200),
    ("Order3", "Bob", 150),
    ("Order4", "Alice", 300),
    ("Order5", "Bob", 250),
    ("Order6", "John", 400)
]

# Create DataFrame
order_df = spark.createDataFrame(order_data).toDF("order_id", "customer", "amount")




df2 = order_df.groupBy("customer").agg(
    count("order_id").alias("count of orders"),
    sum("amount").alias("total order amount")
)
df2.show()

