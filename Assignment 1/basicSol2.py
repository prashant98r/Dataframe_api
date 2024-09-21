# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.ui.port", "4040")
         .appName("spark-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
student=[(1,85),(2, 42),(3, 73)]
columns=["student_id","score"]

grades_df=spark.createDataFrame(student,columns)
grades_result= grades_df.withColumn("grades",when(col("score") >= 50,"Pass").otherwise("Fail"))
grades_result.show()