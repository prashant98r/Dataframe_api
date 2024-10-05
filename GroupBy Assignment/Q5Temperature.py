import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] ="C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"]="C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.eventLog.enabled", "false")
         .appName("spark-orders-program")
         .master("local[*]")
         .getOrCreate())


log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.OFF)


# Sample Data
weatherData = [
    ("City1", "2022-01-01", 10.0),
    ("City1", "2022-01-02", 8.5),
    ("City1", "2022-01-03", 12.3),
    ("City2", "2022-01-01", 15.2),
    ("City2", "2022-01-02", 14.1),
    ("City2", "2022-01-03", 16.8)
]

# Create DataFrame
weather_df = spark.createDataFrame(weatherData).toDF("City", "Date", "Temperature")



weather_of_city = weather_df.groupBy("City").agg(
    min("Temperature").alias("minimum_temp_of_city"),
    max("Temperature").alias("maximum_temp_of_city"),
    avg("Temperature").alias("average_temp_of_city")
)
weather_of_city.show()


weather_df.createOrReplaceTempView("weatherData")

spark.sql("""
            SELECT 
            City,
            Min(Temperature) as minimum_temp_of_city,
            Max(Temperature) as maximum_temp_of_city,
            Avg(Temperature) as average_temp_of_city
            FROM weatherData
            GROUP BY City
""").show()

