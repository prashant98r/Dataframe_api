import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, max, min, count

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.ui.port", "4040")
         .appName("spark-sql-movie-rating")
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

df1 = spark.createDataFrame(movies).toDF("movie_id", "movie_name", "rating", "duration_minutes")


df1.createOrReplaceTempView("movies")

results_df=spark.sql("""
            SELECT 
            *,
                CASE
                    WHEN rating > 8 THEN 'Excellent'
                    WHEN rating >= 6 AND rating <=8 THEN 'Good'
                    ELSE 'Average'
                END as rating_category,
                CASE
                    WHEN duration_minutes > 150 THEN 'Long'
                    WHEN duration_minutes>= 90 AND duration_minutes <= 150 THEN 'Medium'
                    ELSE 'Short'
                END as duration_category     
            FROM movies
""")

results_df.show()



movies_start_with_T = spark.sql("""
        SELECT 
        *
        FROM movies
        WHERE movie_name like "T%"
""")
movies_start_with_T.show()


movies_ends_with_e = spark.sql("""
        SELECT 
        *
        FROM movies
        WHERE movie_name like "%e"
""")
movies_ends_with_e.show()

results_df.createOrReplaceTempView("movies_with_rating_cat")
movies_stats = spark.sql("""
        SELECT 
        rating_category,
        SUM(duration_minutes) as total_duration_minutes,
        AVG(duration_minutes) as average_duration_minutes,
        MAX(duration_minutes) as maximum_duration_minutes,
        MIN(duration_minutes) as minimum_duration_minutes
        FROM movies_with_rating_cat
        GROUP BY rating_category
""")
movies_stats.show()