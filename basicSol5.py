# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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
events=[(1, "2024-07-27"),(2, "2024-12-25"),(3, "2025-01-01")]
#columns=["event_id","date"]

events_df= spark.createDataFrame(events).toDF("event_id","date")
events_with_is_holiday= events_df.withColumn("is_holiday",
                                                       when((col("date")=="2024-12-25" ) | (col("date")=="2025-01-01"),True)
                                                       .otherwise(False))
events_with_is_holiday.show()