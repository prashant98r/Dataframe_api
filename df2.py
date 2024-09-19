# Import necessary libraries
from pyspark.sql import SparkSession
import os

#from pyspark.sql.functions import date_add

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read \
.format("csv") \
.option("header","true") \
.option("inferschema","true") \
.load("C:/Users/kprat/Documents/DataFiles/orders.csv")

df.show()