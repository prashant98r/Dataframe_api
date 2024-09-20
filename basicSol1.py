# Import necessary libraries
import os
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
employee=[(1, "John", 8), (2, "Jane", 35), (3, "Doe", 22)]
columns=("id","name","age")

employees_df=spark.createDataFrame(employee,columns)
employees_with_is_adult= employees_df.withColumn("is_adult",when(col("age") >= 18,True).otherwise(False))
employees_with_is_adult.show()

