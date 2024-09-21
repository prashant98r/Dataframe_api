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

logins =[(1, "09:00"), (2, "18:30"), (3, "14:00")]

logins_df= spark.createDataFrame(logins).toDF("login_id", "login_time")
logins_with_is_morning= logins_df.withColumn(
    "is_morning"
    ,when(hour(to_timestamp(col("login_time"), "HH:mm")) < 12,True).otherwise(False)
)
logins_with_is_morning.show()