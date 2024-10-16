import os
import logging
from pyspark.sql.functions import *
from pyspark.sql import SparkSession,Window


# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] ="C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"]="C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .appName("SalesPerCustomer")
         .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:///path/to/log4j.properties")
         .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:///path/to/log4j.properties")
         .master("local[*]")
         .getOrCreate())


# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting Spark application")

customers = [(1, "Alice", "2023-01-15"),
             (2, "Bob", "2023-02-10"),
             (3, "Charlie", None)]

orders = [(1, "electronics", 300.50, "2023-01-20"),
          (1, "clothing", None, "2023-01-25"),
          (2, "groceries", 120.00, "2023-02-15"),
          (3, "clothing", 50.00, "2023-02-20")]

customers_df= spark.createDataFrame(customers).toDF("customer_id", "customer_name", "signup_date")
orders_df= spark.createDataFrame(orders).toDF("customer_id", "order_type", "sales_amount", "order_date")

order_with_handles_nulls= orders_df.withColumn("sales_amount",coalesce(col("sales_amount"),lit("0.0")))
order_with_handles_nulls.show()

totalSalesDf =order_with_handles_nulls.groupBy("customer_id").agg(
    sum("sales_amount").alias("Total_sales_per_customer")
)
totalSalesDf.show()

categorized_df= totalSalesDf.withColumn("category_of_customers",
                                        when(col("Total_sales_per_customer") >= 250, "High")
                                        .when(col("Total_sales_per_customer") >= 100, "Medium")
                                        .otherwise("Low")
                                        )

joined_df= categorized_df.join(customers_df,"customer_id","left")
joined_df.show()


customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")

spark.sql("""
SELECT c.customer_id, c.customer_name, c.signup_date, 
       COALESCE(SUM(o.sales_amount), 0) as total_sales,
       CASE
           WHEN COALESCE(SUM(o.sales_amount), 0) >= 250 THEN 'High'
           WHEN COALESCE(SUM(o.sales_amount), 0) >= 100 THEN 'Medium'
           ELSE 'Low'
       END as category
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.signup_date
""").show()
