import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] ="C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"]="C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.eventLog.enabled", "false")
         .appName("umulativeSalesExample")
         .master("local[*]")
         .getOrCreate())


log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.OFF)


# Sample Data
ratingData = [
    ("User1", "Movie1", "Action", 4.5),
    ("User1", "Movie2", "Drama", 3.5),
    ("User1", "Movie3", "Comedy", 2.5),
    ("User2", "Movie1", "Action", 3.0),
    ("User2", "Movie2", "Drama", 4.0),
    ("User2", "Movie3", "Comedy", 5.0),
    ("User3", "Movie1", "Action", 5.0),
    ("User3", "Movie2", "Drama", 4.5),
    ("User3", "Movie3", "Comedy", 3.0)
]

# Create DataFrame
rating_df = spark.createDataFrame(ratingData).toDF("User", "Movie", "Genre", "Rating")


avg_ratings_df = rating_df.groupBy("User", "Genre").agg(
    avg("Rating").alias("AverageRating")
)

avg_ratings_df.show()

# Create a temporary view for SQL queries
rating_df.createOrReplaceTempView("rating_data")

# SQL query to calculate the average rating for each user and genre
spark.sql("""
    SELECT 
        User, 
        Genre, 
        AVG(Rating) AS AverageRating
    FROM rating_data
    GROUP BY User, Genre
""").show()


