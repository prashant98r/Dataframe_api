import os
from os import truncate

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] ="C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"]="C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.eventLog.enabled", "false")
         .appName("WordCountExample")
         .master("local[*]")
         .getOrCreate())


log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.OFF)


# Sample Data
textData = [
    ("Hello, how are you?",),
    ("I am fine, thank you!",),
    ("How about you?",)
]

# Create DataFrame
text_df = spark.createDataFrame(textData ).toDF("Text")
text_df.show(truncate=False)

words_df = text_df.select(
    explode(
        split(col("Text"), r"\W+")  # Splitting the text into words using a regex for non-word characters
    ).alias("Word")
).filter(col("Word") != "")  # Filtering out any empty strings

words_df.show()


word_count_df = words_df.groupBy("Word").count().alias("Count")

# Show the result
word_count_df.show()


text_df.createOrReplaceTempView("text_data")


# Explode and split into individual words using Spark SQL
spark.sql("""
    SELECT
        EXPLODE(SPLIT(Text, '\\\\W+')) AS Word
    FROM text_data
""").createOrReplaceTempView("words")

# Query to count occurrences of each word
spark.sql("""
    SELECT 
        Word, 
        COUNT(Word) AS Count
    FROM words
    WHERE Word != ''
    GROUP BY Word
""").show(truncate=False)

