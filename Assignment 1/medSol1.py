# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/Users/kprat/Documents/spark-3.3.1-bin-hadoop3/spark-3.3.1-bin-hadoop3"

# Create a SparkSession
spark = (SparkSession.builder
         .config("spark.ui.port", "4040")
         .appName("spark-program")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Sample inventory data
inventory = [(1, 5), (2, 15), (3, 25)]

# Create DataFrame with proper column names
inventory_df = spark.createDataFrame(inventory).toDF("item_id", "quantity")

# Add a 'stock_level' column using select and when condition
inventory_with_stock_level = inventory_df.select(
    col("item_id"),
    col("quantity"),
    when(col("quantity") < 10, "Low")
        .when((col("quantity") >= 10) & (col("quantity") <= 20), "Medium")
        .otherwise("High")
        .alias("stock_level")
)

# Show the final DataFrame
inventory_with_stock_level.show()
