# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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
products=[(1, 30.5),(2, 150.75),(3, 75.25)]

products_df= spark.createDataFrame(products).toDF("product_id","price")
products_with_price_range= products_df.withColumn("price_range",
                                                       when(col("price") < 50,"Cheap")
                                                       .when((col("price")>=50) & (col("price")<=100),"Moderate")
                                                       .otherwise("Expensive"))
products_with_price_range.show()