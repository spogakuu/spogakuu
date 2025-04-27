    from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark
spark = SparkSession.builder.appName("Create STAGING_MAIN_INSERT_LOGS").getOrCreate()

# Define schema
main_log_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("load_date", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("error_message", StringType(), True)
])

# Create empty DataFrame
empty_main_log_df = spark.createDataFrame([], main_log_schema)

# Save to path
empty_main_log_df.write.mode("overwrite").parquet("Files/Bronze/STAGING/LOGS/STAGING_MAIN_INSERT_LOGS")

print("✅ STAGING_MAIN_INSERT_LOGS created successfully.")