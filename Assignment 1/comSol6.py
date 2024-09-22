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

weather=[(1, 25, 60), (2, 35, 40), (3, 15, 80)]

weather_df= spark.createDataFrame(weather).toDF("day_id", "temperature","humidity")

weather_with_column_df= (weather_df.withColumn(
    "is_hot"
    ,when(col("temperature")>30,True)
    .otherwise(False)).withColumn("is_humid",when(col("humidity")>35,True)
    .otherwise(False))
)

weather_with_column_df.show()