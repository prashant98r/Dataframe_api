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

employees =[(1, 25, 30000), (2, 45, 50000), (3, 35, 40000) ]

employees_df= spark.createDataFrame(employees).toDF("employee_id", "age","salary")
employees_with_category= (employees_df.withColumn(
    "category"
    ,when((col("age")<30) & (col("salary") < 35000),"Young & Low Salary")
    .when((col("age").between(30,40)) & (col("salary").between(35000,45000)),"Middle Aged & Medium Salary")
    .otherwise("Old & High Salary"))
)
employees_with_category.show()