# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import datediff

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

emails=[(1, "user@gmail.com"), (2, "admin@yahoo.com"), (3, "info@hotmail.com")]

emails_df= spark.createDataFrame(emails).toDF("email_id", "email_address")

emails_with_column_df= (emails_df.withColumn(
    "email_domain"
    ,when(col("email_address").contains("gmail"),"Gmail")
    .when(col("email_address").contains("yahoo"),"Yahoo")
    .otherwise("Hotmail"))
)


emails_with_column_df.show()