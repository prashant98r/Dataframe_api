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
         .appName("spark-sql-student-analysis")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Sample Data
products = [
    (1, "Smartphone", 700, "Electronics"),
    (2, "TV", 1200, "Electronics"),
    (3, "Shoes", 150, "Apparel"),
    (4, "Socks", 25, "Apparel"),
    (5, "Laptop", 800, "Electronics"),
    (6, "Jacket", 200, "Apparel")
]

df1 = spark.createDataFrame(products).toDF("product_id","product_name","price","category")

df1.createOrReplaceTempView("products")

spark.sql("""
            SELECT 
            *,
                CASE
                    WHEN price > 500 THEN 'Expensive'
                    WHEN price >= 200 AND price <=500 THEN 'Moderate'
                    ELSE 'Cheap'
                END as price_category
            FROM products


""").show()

spark.sql("""
        SELECT 
        *
        FROM products
        WHERE product_name like "S%"
""").show()

spark.sql("""
        SELECT 
        *
        FROM products
        WHERE product_name like "%s"
""").show()

spark.sql("""
        SELECT 
        category,
        SUM(price) as total_price,
        AVG(price) as average_price,
        MAX(price) as maximum_price,
        MIN(price) as minimum_price
        FROM products
        GROUP BY category
""").show()