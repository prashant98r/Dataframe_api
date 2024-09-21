# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, initcap

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

customers=[(1, "john@gmail.com"), (2, "jane@yahoo.com"), (3, "doe@hotmail.com")]

customers_df= spark.createDataFrame(customers).toDF("customer_id", "email")
customers_with_email_provider= customers_df.withColumn(
    "email_provider"
    ,when(col("email").contains("gmail"),"Gmail" )
    .when(col("email").contains("yahoo"),"Yahoo")
    .otherwise("Other")
)
customers_with_email_provider.show()