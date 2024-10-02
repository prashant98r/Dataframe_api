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
         .appName("spark-ecom-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Sample Data
employees = [
    (1, "John", 28, 60000),
    (2, "Jane", 32, 75000),
    (3, "Mike", 45, 120000),
    (4, "Alice", 55, 90000),
    (5, "Steve", 62, 110000),
    (6, "Claire", 40, 40000)
]

# Create DataFrame
employees_df=spark.createDataFrame(employees).toDF("employee_id", "name", "age", "salary")

employees_with_age_grp=employees_df.withColumn("age_group"
                                               ,when(col("age") < 30,"Young")
                                               .when((col("age")>=30) & (col("age")<=50),"Mid")
                                               .otherwise("Senior")
                                                ).withColumn("salary_range",
                                                when(col("salary")>100000,"High")
                                                .when((col("salary")>=50000) & (col("salary")<=100000),"Medium")
                                                .otherwise("Low")
)


employees_with_age_grp.show()

starts_with_j= employees_df.filter(col("name").startswith('J'))
starts_with_j.show()

ends_with_e= employees_df.filter(col("name").endswith('e'))
ends_with_e.show()


salary_stats=employees_with_age_grp.groupBy("age_group").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("average_salary"),
    max("salary").alias("maximum_salary"),
    min("salary").alias("minimum_salary"),
)
salary_stats.show()

