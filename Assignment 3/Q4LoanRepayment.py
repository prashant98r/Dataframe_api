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
         .appName("Loan Repayment App")
         .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:///path/to/log4j.properties")
         .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:///path/to/log4j.properties")
         .master("local[*]")
         .getOrCreate())


# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting Spark application")
try:
    loanRepayment = [
        ("L001", "C001", 1000, "2023-11-01", "2023-12-01", "Personal Loan", 7.5),
        ("L002", "C001", 1500, "2023-12-15", "2024-01-10", "Personal Loan", 7.5),
        ("L003", "C001", 2000, "2024-01-20", "2024-02-15", "Home Loan", 6.5),

        ("L004", "C002", 1200, "2023-11-01", "2023-11-30", "Car Loan", 8.0),
        ("L005", "C002", 1800, "2023-12-05", "2023-12-30", "Car Loan", 8.5),
        ("L006", "C002", 1400, "2024-01-10", "2024-01-25", "Car Loan", 8.0),

        ("L007", "C003", 2200, "2023-12-20", "2024-01-05", "Personal Loan", 7.9),
        ("L008", "C003", 2500, "2024-01-15", "2024-02-10", "Personal Loan", 8.2),
        ("L009", "C003", 2600, "2024-02-20", "2024-03-10", "Home Loan", 6.8),

        ("L010", "C004", 1700, "2023-11-25", "2023-12-20", "Personal Loan", 7.6),
        ("L011", "C004", 2100, "2024-01-01", "2024-01-25", "Personal Loan", 7.9),
        ("L012", "C004", 2300, "2024-02-10", "2024-03-05", "Home Loan", 6.8)
    ]

    loan_df = spark.createDataFrame(loanRepayment).toDF("loan_id", "customer_id", "repayment_amount", "due_date",
                                                        "payment_date", "loan_type", "interest_rate")

    loan_df = loan_df.withColumn("payment_date", to_date("payment_date")).withColumn("due_date", to_date("due_date"))
    loan_df.show()

    repayment_df = loan_df.withColumn("repayment_delay", datediff(col("payment_date"), col("due_date")))
    repayment_df.show()

    filtered_df = repayment_df.filter(col("loan_type").startswith("Personal") & (col("repayment_delay") > 30))
    filtered_df.show()

    aggregated_df = repayment_df.groupBy("loan_type", "interest_rate").agg(
        sum("repayment_amount").alias("Total_repayment_amount_collected"),
        max("repayment_delay").alias("Maximum repayment delay"),
        avg("interest_rate").alias("average_interest_rate_for_delayed_payments")
    )
    aggregated_df.show()

    window_spec = Window.partitionBy("customer_id").orderBy("due_date")
    df_with_lead = repayment_df.withColumn("next_repayment_amount", lead("repayment_amount", 1).over(window_spec))
    df_with_lead.show()

    loan_df.createOrReplaceTempView("loanRepayment")

    repayment_df = spark.sql("""
            SELECT *, 
                    DATEDIFF(payment_date, due_date) as repayment_delay 
            FROM loanRepayment
    """)
    repayment_df.createOrReplaceTempView("repayment_delays")
    repayment_df.show()

    filtered_df = spark.sql("""
            SELECT * 
            FROM repayment_delays
            WHERE loan_type LIKE 'Personal%' AND repayment_delay > 30
    """
                            )
    filtered_df.show()

    aggregated_df = spark.sql("""
        SELECT loan_type, interest_rate, 
               SUM(repayment_amount) AS Total_repayment_amount_collected,
               MAX(repayment_delay) AS Maximum_repayment_delay,
               AVG(interest_rate) AS average_interest_rate_for_delayed_payments
        FROM repayment_delays
        GROUP BY loan_type, interest_rate
    """)
    aggregated_df.show()

    spark.sql("""
        SELECT 
        *,
        Lead(repayment_amount) OVER (PARTITION BY customer_id ORDER BY due_date) as next_repayment_amount           
        FROM repayment_delays
    """).show()

    logger.info("Spark application completed successfully")

except Exception as e:
    logger.error(f"Error occurred: {e}")





