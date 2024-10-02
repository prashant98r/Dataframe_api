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
         .appName("spark-ecom-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Sample Data
products = [
    (1, "Smartphone", 700, "Electronics"),
    (2, "TV", 1200, "Electronics"),
    (3, "Shoes", 150, "Apparel"),
    (4, "Socks", 25, "Apparel"),
    (5, "Laptop", 800, "Electronics"),
    (6, "Jacket", 200, "Apparel")
]

# Create DataFrame
products_df=spark.createDataFrame(products).toDF("product_id","product_name","price","category")

products_with_price_cat=(products_df.withColumn("price_category"
                                               ,when(col("price") > 500,"Expensive")
                                               .when((col("price")>=200) & (col("price")<=500),"Moderate")
                                               .otherwise("Cheap")
                                                )
 ).show()

starts_with_s= products_df.filter(col("product_name").startswith('S')).show()
ends_with_s= products_df.filter(col("product_name").endswith('s')).show()


price_stats=products_df.groupBy("category").agg(
    sum("price").alias("total_price"),
    avg("price").alias("average_price"),
    max("price").alias("maximum_price"),
    min("price").alias("minimum_price"),
).show()

