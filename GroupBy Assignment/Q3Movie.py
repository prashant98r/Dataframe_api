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
ratings_data = [
    ("User1", "Movie1", 4.5),
    ("User2", "Movie1", 3.5),
    ("User3", "Movie2", 2.5),
    ("User4", "Movie2", 3.0),
    ("User1", "Movie3", 5.0),
    ("User2", "Movie3", 4.0)
]

# Create DataFrame
ratings_df = spark.createDataFrame(ratings_data ).toDF("User", "Movie", "Rating")




Movie_df = ratings_df.groupBy("Movie").agg(
    avg("Rating").alias("average_rating"),
    count("Rating").alias("Total_rating")
)
Movie_df.show()


ratings_df.createOrReplaceTempView("ratings_data")

spark.sql("""
            SELECT 
            Movie,
            Avg(Rating) as average_rating,
            Count(Rating) as Total_rating
            FROM ratings_data
            GROUP BY Movie
""").show()

