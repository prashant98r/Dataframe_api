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
employees = [
    (1, "John", 28, 60000),
    (2, "Jane", 32, 75000),
    (3, "Mike", 45, 120000),
    (4, "Alice", 55, 90000),
    (5, "Steve", 62, 110000),
    (6, "Claire", 40, 40000)
]

df1 = spark.createDataFrame(employees).toDF("employee_id", "name", "age", "salary")

df1.createOrReplaceTempView("employees")

spark.sql("""
            SELECT 
            *,
                CASE
                    WHEN age < 30 THEN 'Young'
                    WHEN age >= 30 AND age <=50 THEN 'Mid'
                    ELSE 'Senior'
                END as age_group,
                CASE
                    WHEN salary > 100000 THEN 'High'
                    WHEN salary >= 50000 AND salary <= 100000 THEN 'Medium'
                    ELSE 'Low'
                END as salary_range        
            FROM employees
""").createOrReplaceTempView("employees_with_groups")

employees_start_with_j = spark.sql("""
        SELECT 
        *
        FROM employees
        WHERE name like "J%"
""")
employees_start_with_j.show()


employees_ends_with_e = spark.sql("""
        SELECT 
        *
        FROM employees
        WHERE name like "%e"
""")
employees_ends_with_e.show()


salary_stats = spark.sql("""
        SELECT 
        age_group,
        SUM(salary) as total_salary,
        AVG(salary) as average_salary,
        MAX(salary) as maximum_salary,
        MIN(salary) as minimum_salary
        FROM employees_with_groups
        GROUP BY age_group
""")
salary_stats.show()