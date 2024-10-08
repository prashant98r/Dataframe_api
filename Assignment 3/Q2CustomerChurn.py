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
customerChurn_data = [
    ("C001", "Premium Gold", "Yes", "2023-12-01", 1200, "USA"),
    ("C002", "Basic", "No", None, 400, "Canada"),
    ("C003", "Premium Silver", "Yes", "2023-11-15", 800, "UK"),
    ("C004", "Premium Gold", "Yes", "2024-01-10", 1500, "USA"),
    ("C005", "Basic", "No", None, 300, "India"),
    ("C006", "Premium Silver", "Yes", "2024-01-20", 1300, "UK"),
    ("C007", "Premium Gold", "Yes", "2024-02-15", 1800, "USA"),
    ("C008", "Basic", "No", None, 600, "India"),
    ("C009", "Premium Gold", "Yes", "2023-12-25", 1700, "USA"),
    ("C010", "Premium Silver", "Yes", "2023-11-01", 900, "UK")
]


# Create DataFrame
customerChurn_df = spark.createDataFrame(customerChurn_data).toDF("customer_id", "subscription_type", "churn_status", "churn_date", "revenue","country")

customerChurn_df = customerChurn_df.withColumn("churn_date",to_date(col("churn_date"),"yyyy-MM-dd"))
customerChurn_df =  customerChurn_df.withColumn("churn_Year",year(col("churn_date")))
customerChurn_df.show()

filtered_df= customerChurn_df.filter(
    (col("subscription_type").startswith('Premium')) & (col("churn_status").isNotNull())
)

filtered_df.show()

aggregated_df= customerChurn_df.groupBy("country","churn_year").agg(
    sum("revenue").alias("Total_revenue_lost"),
    avg("revenue").alias("Average_revenue"),
    count("customer_id").alias("Count_of_churned_customers")
)
aggregated_df.show()

window_spec=Window.partitionBy("country").orderBy("churn_year")
result_df =aggregated_df.withColumn("Next_years_revenue_trend",lead(col("Total_revenue_lost"),1).over(window_spec))

result_df.show()


# Create a temporary view for SQL queries
customerChurn_df.createOrReplaceTempView("customer_churn")

# SQL query to calculate the average performance score per department and review month
spark.sql("""
    WITH filtered_churn AS (
        SELECT 
            customer_id, 
            subscription_type, 
            churn_status, 
            churn_date, 
            revenue, 
            country,
            YEAR(churn_date) as churn_year
        FROM customer_churn
        WHERE subscription_type LIKE 'Premium%' AND churn_status IS NOT NULL
)
        SELECT 
            country, 
            churn_year, 
            SUM(revenue) AS total_revenue_lost, 
            AVG(revenue) AS avg_revenue_per_churn, 
            COUNT(customer_id) AS churned_customers_count,
            LEAD(SUM(revenue)) OVER (PARTITION BY country ORDER BY churn_year) AS next_year_revenue
        FROM filtered_churn
        GROUP BY country, churn_year
        ORDER BY country, churn_year 
""").show()


