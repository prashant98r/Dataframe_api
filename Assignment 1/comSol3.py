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

documents=[(1, "The quick brown fox"), (2, "Lorem ipsum dolor sit amet"), (3, "Spark is a unified analytics engine")]

documents_df= spark.createDataFrame(documents).toDF("doc_id", "content")
documents_with_column= (documents_df.withColumn(
    "content_category"
    ,when(col("content").contains("fox"),"Animal Related")
    .when(col("content").contains("Lorem"),"Placeholder Text")
    .when(col("content").contains("Spark"),"Tech Related"))
)
documents_with_column.show(truncate=False)