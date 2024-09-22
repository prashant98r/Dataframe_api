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

reviews =[(1, 1), (2, 4), (3, 5)]

reviews_df= spark.createDataFrame(reviews).toDF("review_id", "rating")
reviews_with_column= (reviews_df.withColumn(
    "feedback"
    ,when(col("rating")<3,"Bad")
    .when(col("rating").isin(3,4),"Good")
    .when(col("rating")==5,"Excellent")
).withColumn(
    "is_positive"
    ,when(col("rating")>=3,True)
    .otherwise(False))
)
reviews_with_column.show()