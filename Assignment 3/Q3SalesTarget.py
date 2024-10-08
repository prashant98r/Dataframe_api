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
salesTargetAchievement  = [
    ("S001", 15000, 12000, "2023-12-10", "North", "Electronics Accessories"),
    ("S001", 18000, 17000, "2023-12-15", "North", "Electronics Accessories"),
    ("S001", 20000, 19000, "2023-12-20", "North", "Electronics Accessories"),
    ("S002", 8000, 9000, "2023-12-11", "South", "Home Appliances"),
    ("S002", 12000, 10000, "2023-12-14", "South", "Home Appliances"),
    ("S003", 20000, 18000, "2023-12-12", "East", "Electronics Gadgets"),
    ("S003", 22000, 20000, "2023-12-16", "East", "Electronics Gadgets"),
    ("S004", 10000, 15000, "2023-12-13", "West", "Electronics Accessories"),
    ("S004", 13000, 14000, "2023-12-17", "West", "Electronics Accessories"),
    ("S005", 18000, 15000, "2023-12-14", "North", "Furniture Accessories"),
    ("S006", 9000, 8000, "2023-12-15", "North", "Electronics Accessories"),
    ("S007", 13000, 11000, "2023-12-16", "East", "Electronics Accessories"),
    ("S008", 12000, 11000, "2023-12-17", "West", "Electronics Accessories"),
    ("S009", 22000, 18000, "2023-12-18", "South", "Electronics Gadgets"),
    ("S010", 11000, 10000, "2023-12-19", "West", "Electronics Accessories")
]


# Create DataFrame
sales_df = spark.createDataFrame(salesTargetAchievement).toDF(
    "salesperson_id", "sales_amount", "target_amount", "sale_date", "region", "product_category")

sales_df = sales_df.withColumn("target_achieved", col("sales_amount") >= col("target_amount"))
sales_df.show()


filtered_df = sales_df.filter(
    col("product_category").like("Electronics%Accessories")
)
filtered_df.show()


aggregated_df = sales_df.groupBy("region", "product_category").agg(
    sum("sales_amount").alias("total_sales_amount"),
    min("sales_amount").alias("min_sales_amount"),
    count(when(col("target_achieved") == True, 1)).alias("num_achieved_targets")
)
aggregated_df.show()



window_spec = Window.partitionBy("salesperson_id").orderBy("sale_date")
sales_with_lag = sales_df.withColumn("previous_sales", lag("sales_amount", 1).over(window_spec))

sales_with_lag.show()




sales_df.createOrReplaceTempView("sales_targets")

# SQL query to perform the required operations
spark.sql("""
    WITH filtered_sales AS (
        SELECT 
            salesperson_id, 
            sales_amount, 
            target_amount, 
            sale_date, 
            region, 
            product_category,
            (CASE WHEN sales_amount >= target_amount THEN TRUE ELSE FALSE END) AS target_achieved
        FROM sales_targets
        WHERE product_category LIKE 'Electronics%Accessories'
    )
    SELECT 
        region, 
        product_category,
        SUM(sales_amount) AS total_sales_amount,
        MIN(sales_amount) AS min_sales_amount,
        COUNT(CASE WHEN target_achieved = TRUE THEN 1 END) AS num_achieved_targets
    FROM filtered_sales
    GROUP BY region, product_category
    ORDER BY region
""").show()

# SQL query to compare each salesperson's current sales with their previous sales using lag function
spark.sql("""
    SELECT 
        salesperson_id, 
        sales_amount, 
        sale_date,
        LAG(sales_amount, 1) OVER (PARTITION BY salesperson_id ORDER BY sale_date) AS previous_sales
    FROM sales_targets
""").show()