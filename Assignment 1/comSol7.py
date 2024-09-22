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

scores=[(1, 85, 92), (2, 58, 76), (3, 72, 64)]

scores_df= spark.createDataFrame(scores).toDF("student_id", "maths_score","english_score")

scores_with_column_df= (scores_df.withColumn(
    "math_grade"
    ,when(col("maths_score")>80,"A")
    .when(col("maths_score").between(60,80),"B")
    .otherwise("C")).withColumn("english_grade",when(col("english_score")>80,"A")
                                .when(col("english_score").between(60,80),"B")
                                .otherwise("C"))
)

scores_with_column_df.show()