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
purchaseData = [
    ("Customer1", "Product1", 100),
    ("Customer1", "Product2", 150),
    ("Customer1", "Product3", 200),
    ("Customer2", "Product2", 120),
    ("Customer2", "Product3", 180),
    ("Customer3", "Product1", 80),
    ("Customer3", "Product3", 250)
]

# Create DataFrame
purchase_df = spark.createDataFrame(purchaseData).toDF("Customer", "Product", "Amount")

customer_stats_df = purchase_df.groupBy("Customer").agg(
    countDistinct("Product").alias("Distinct_Products"),
    sum("Amount").alias("Total_Purchase_Amount")
)

customer_stats_df.show()


purchase_df.createOrReplaceTempView("purchase_data")

spark.sql("""
        SELECT 
            Customer,
            COUNT(DISTINCT Product) AS Distinct_Products, 
            SUM(Amount) AS Total_Purchase_Amount
        FROM purchase_data
        GROUP BY Customer
""").show()


