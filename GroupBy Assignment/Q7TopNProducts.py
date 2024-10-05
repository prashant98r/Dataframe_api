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


# Sample Data
salesData = [
    ("Product1", 100),
    ("Product2", 200),
    ("Product3", 150),
    ("Product4", 300),
    ("Product5", 250),
    ("Product6", 180)
]


# Create DataFrame
sales_df = spark.createDataFrame(salesData).toDF("Product", "SalesAmount")

n=4

top_n_products = sales_df.orderBy(col("SalesAmount").desc()).limit(n)
top_n_products.show()


sales_df.createOrReplaceTempView("salesData")

spark.sql(f"""
        SELECT 
            Product,
            SalesAmount 
        FROM salesData
        Order BY SalesAmount DESC 
        LIMIT {n}
""").show()


