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
         .appName("spark-movie-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Sample Data
movies = [
    (1, "The Matrix", 9, 136),
    (2, "Inceptione", 8, 148),
    (3, "The Godfather", 5, 175),
    (4, "Toy Story", 7, 81),
    (5, "The Shawshank Redemptione", 10, 142),
    (6, "The Silence of the Lambs", 8, 118)
]

# Create DataFrame
movies_df = spark.createDataFrame(movies).toDF("movie_id", "movie_name", "rating", "duration_minutes")

movies_with_rating_cat = movies_df.withColumn("rating_category"
                                               ,when(col("rating") > 8,"Excellent")
                                               .when((col("rating") >= 6) & (col("rating") <= 8),"Good")
                                               .when(col("rating") <6,"Average")
                                                ).withColumn("duration_category",
                                                when(col("duration_minutes") > 150,"Long")
                                                .when((col("duration_minutes") >= 90) & (col("duration_minutes") <= 150),"Medium")
                                                .when(col("duration_minutes") <90,"Short")
)


movies_with_rating_cat.show()

starts_with_T= movies_df.filter(col("movie_name").startswith('T'))
starts_with_T.show()

ends_with_e= movies_df.filter(col("movie_name").endswith('e'))
ends_with_e.show()


movie_duration_stats = movies_with_rating_cat.groupBy("rating_category").agg(
    sum("duration_minutes").alias("total_duration_minutes"),
    avg("duration_minutes").alias("average_duration_minutes"),
    max("duration_minutes").alias("maximum_duration_minutes"),
    min("duration_minutes").alias("minimum_duration_minutes"),
)
movie_duration_stats.show()

