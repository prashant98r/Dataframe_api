# Import necessary libraries
import os

from unicodedata import category

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.ui.port", "4040")
         .appName("spark-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
transactions=[(1, 1000),(2, 200),(3, 5000)]
columns=["transaction_id","amount"]

transactions_df= spark.createDataFrame(transactions,columns)
transactions_with_category= transactions_df.withColumn("category",
                                                       when(col("amount") > 1000,"High")
                                                       .when((col("amount")>=500) & (col("amount")<=1000),"Medium")
                                                       .otherwise("Low"))
transactions_with_category.show()