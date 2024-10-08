import os
from itertools import groupby

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] ="C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"]="C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.eventLog.enabled", "false")
         .appName("EmployeePerformanceAnalysis")
         .master("local[*]")
         .getOrCreate())


log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.OFF)


# Sample Data
employee_performance_data = [
    ("E001", "Sales", 85, "2024-02-10", "Sales Manager"),
    ("E002", "HR", 78, "2024-03-15", "HR Assistant"),
    ("E003", "IT", 92, "2024-01-22", "IT Manager"),
    ("E004", "Sales", 88, "2024-02-18", "Sales Rep"),
    ("E005", "HR", 95, "2024-03-20", "HR Manager"),
    ("E006", "Finance", 81, "2024-02-25", "Finance Manager"),
    ("E007", "IT", 83, "2024-01-30", "IT Support"),
    ("E008", "Sales", 91, "2024-04-05", "Sales Manager"),
    ("E009", "Marketing", 93, "2024-02-11", "Marketing Manager"),
    ("E010", "IT", 88, "2024-03-22", "IT Manager"),
    ("E011", "HR", 89, "2024-03-05", "HR Manager"),
    ("E012", "Sales", 82, "2024-01-18", "Sales Rep"),
    ("E013", "Finance", 76, "2024-01-30", "Finance Assistant"),
    ("E014", "Marketing", 86, "2024-03-10", "Marketing Analyst"),
    ("E015", "Sales", 94, "2024-02-20", "Sales Manager"),
    ("E016", "IT", 90, "2024-04-02", "IT Manager"),
    ("E017", "HR", 77, "2024-01-18", "HR Assistant"),
    ("E018", "Sales", 83, "2024-03-14", "Sales Manager"),
    ("E019", "Finance", 87, "2024-02-22", "Finance Manager"),
    ("E020", "Marketing", 91, "2024-01-30", "Marketing Manager")
]


# Create DataFrame
employee_df = spark.createDataFrame(employee_performance_data).toDF("employee_id", "department", "performance_score", "review_date", "position")


employee_df = employee_df.withColumn("review_date",to_date(col("review_date"),"yyyy-MM-dd"))
employee_df=  employee_df.withColumn("review_month",month(col("review_date")))
employee_df.show()

manager_df= employee_df.filter(
    (col("position").endswith('Manager')) & (col("performance_score") > 80))
manager_df.show()

performance_summary_df= employee_df.groupBy("department","review_month").agg(
    avg("performance_score").alias("Average_performance_score"),
    count(when(col("performance_score")>90,True)).alias("High_performance_count")
)
performance_summary_df.show()

window_spec=Window.orderBy("review_date")
employee_with_lag_df =employee_df.withColumn("Previous_performance_score",lag(col("performance_score"),1).over(window_spec))

employee_with_lag_df = employee_with_lag_df.withColumn(
    "performance_change", col("performance_score") - col("previous_performance_score")
)

employee_with_lag_df.show()


# Create a temporary view for SQL queries
employee_df.createOrReplaceTempView("employee_performance")

# SQL query to calculate the average performance score per department and review month
spark.sql("""
    SELECT 
        department, 
        review_month, 
        AVG(performance_score) AS avg_performance_score,
        COUNT(CASE WHEN performance_score > 90 THEN 1 END) AS high_performance_count
    FROM employee_performance
    WHERE position LIKE '%Manager' AND performance_score > 80
    GROUP BY department, review_month
""").show()

# SQL query to calculate the performance improvement or decline
spark.sql("""
    SELECT 
        employee_id, 
        department, 
        performance_score, 
        review_date, 
        LAG(performance_score, 1) OVER (ORDER BY review_date) AS previous_performance_score,
        (performance_score - LAG(performance_score, 1) OVER (ORDER BY review_date)) AS performance_change
    FROM employee_performance
""").show()
