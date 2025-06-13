# SNAPSHOT_LOGIC with Lock Handling: threadpool + re-run + hash_value + file lock

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sha2, concat_ws, when, lit, date_format
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz
import threading
import traceback
import os

# Initialize Spark
spark = SparkSession.builder.appName("Process Incremental Tables").getOrCreate()
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

# Config Paths
staging_path = "Files/BRONZE/INCREMENTAL_LOAD"
copy_path = "Files/BRONZE/SNAPSHOT/INCREMENTAL"
table_list_path = "Files/TABLES_LIST"
daily_log_path = "Files/BRONZE/SNAPSHOT/LOGS/Daily_Incremental_Logs"
main_log_path = "Files/BRONZE/SNAPSHOT/LOGS/Final_Incremental_Logs"

# Lock file path
lock_file = "/mnt/data/update_log.lock"

# Date
tz = pytz.timezone("US/Central")
Today_date = datetime.now(tz).strftime("%Y-%m-%d")

# Acquire lock
def acquire_lock():
    if os.path.exists(lock_file):
        return False
    with open(lock_file, 'w') as f:
        f.write("locked at " + datetime.now().isoformat())
    return True

# Release lock
def release_lock():
    if os.path.exists(lock_file):
        os.remove(lock_file)

# Thread-safe log write with lock
def update_log(table_name, status, start_time, end_time, error_message=""):
    new_row = [(table_name, status, Today_date, start_time.isoformat(), end_time.isoformat(), error_message)]
    try:
        schema = spark.read.parquet(daily_log_path).schema
    except:
        try:
            schema = spark.read.parquet(main_log_path).schema
            spark.createDataFrame([], schema).write.mode("overwrite").parquet(daily_log_path)
        except Exception as ex:
            print("Failed to initialize daily log:", ex)
            return
    new_df = spark.createDataFrame(new_row, schema)

    if acquire_lock():
        try:
            new_df.write.mode("append").parquet(daily_log_path)
        finally:
            release_lock()
    else:
        print(f"[WAIT] Another process is writing logs. Try again later.")

# Add ROW_HASH without modifying original data, null → space
def add_hash_column(df):
    cleaned_cols = []
    for field in df.schema.fields:
        col_name = field.name
        cleaned_col = when(col(col_name).isNull(), lit(" ")).otherwise(col(col_name).cast("string"))
        cleaned_cols.append(cleaned_col.alias(col_name))
    temp_df = df.select(*cleaned_cols)
    row_hash_col = sha2(concat_ws("||", *temp_df.columns), 256).alias("ROW_HASH")
    return df.withColumn("ROW_HASH", row_hash_col)

# Load and process each table
def load_table(table_name, key_column):
    thread_name = threading.current_thread().name
    start_time = datetime.now()
    print(f"START: [{start_time}] [Thread: {thread_name}] {table_name}")

    try:
        copy_table_path = f"{copy_path}/{table_name}_INC"
        temp_table_path = f"{copy_path}/{table_name}_INC_TEMP"
        insert_path = f"{staging_path}/{table_name}_INSERT"
        update_path = f"{staging_path}/{table_name}_UPDATE"
        delete_path = f"{staging_path}/{table_name}_DELETE"

        spark.catalog.refreshByPath(copy_table_path)
        copy_table = spark.read.parquet(copy_table_path)
        insert_df = spark.read.parquet(insert_path)
        update_df = spark.read.parquet(update_path)
        delete_df = spark.read.parquet(delete_path)

        if not insert_df.rdd.isEmpty():
            insert_view = insert_df.filter(date_format(col("INSERT_TIMESTAMP"), "yyyy-MM-dd") == Today_date)
            copy_table = copy_table.unionByName(insert_view, allowMissingColumns=True)

        if not update_df.rdd.isEmpty():
            update_view = update_df.filter(date_format(col("INSERT_TIMESTAMP"), "yyyy-MM-dd") == Today_date)
            copy_table = copy_table.unionByName(update_view, allowMissingColumns=True)

        if not delete_df.rdd.isEmpty():
            delete_view = delete_df.filter(date_format(col("INSERT_TIMESTAMP"), "yyyy-MM-dd") == Today_date)
            copy_table = copy_table.join(delete_view, copy_table[key_column] == delete_view[key_column], "left_anti")

        # Add row hash
        copy_table = add_hash_column(copy_table)

        # Clean temp path before write
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        temp_path = spark._jvm.org.apache.hadoop.fs.Path(temp_table_path)
        if fs.exists(temp_path):
            fs.delete(temp_path, True)
            print(f"Deleted temp path before write: {temp_table_path}")

        # Write temp table
        copy_table.write.mode("overwrite").parquet(temp_table_path)

        # Rename to final table path
        original_path = spark._jvm.org.apache.hadoop.fs.Path(copy_table_path)
        if fs.exists(original_path):
            fs.delete(original_path, True)
        fs.rename(temp_path, original_path)
        print(f"{table_name} - Temp moved to: {copy_table_path}")

        end_time = datetime.now()
        update_log(table_name, "SUCCESS", start_time, end_time)

    except Exception as e:
        end_time = datetime.now()
        print(f"{table_name} - Error: {e}")
        update_log(table_name, "FAILURE", start_time, end_time, traceback.format_exc())

# Load table list
table_list_df = spark.read.parquet(table_list_path)
active_tables_df = table_list_df.filter("STATUS = 'A'")
active_tables = [(row["TABLE_NAME"], row["KEY_COLUMN"]) for row in active_tables_df.collect()]

# Decide which tables to run
try:
    main_log_df = spark.read.parquet(main_log_path)
    failed_today_df = main_log_df.filter(f"load_date = '{Today_date}' AND status = 'FAILURE'")
    failed_today_tables = [row["table_name"] for row in failed_today_df.collect()]
    main_logged_today_tables = main_log_df.filter(f"load_date = '{Today_date}'").select("table_name").distinct().rdd.flatMap(lambda x: x).collect()

    if failed_today_tables:
        tables_to_run = [item for item in active_tables if item[0] in failed_today_tables]
        print("Retrying failed tables only:", [t[0] for t in tables_to_run])
    elif set([x[0] for x in active_tables]).issubset(set(main_logged_today_tables)):
        tables_to_run = []
        print("All active tables already processed.")
    else:
        tables_to_run = active_tables
        print("Running all active tables.")
except:
    print("Main log not found or error occurred. Running all tables.")
    tables_to_run = active_tables

# Execute tables
if tables_to_run:
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(load_table, tbl, key): tbl for tbl, key in tables_to_run}
        for future in as_completed(futures):
            print("LOOP END")

# Merge daily log to final log
try:
    main_log_df = spark.read.parquet(main_log_path)
    spark.catalog.refreshByPath(daily_log_path)
    daily_log_df = spark.read.parquet(daily_log_path)
except:
    print("Daily log missing, using empty DataFrame.")
    daily_log_df = spark.createDataFrame([], main_log_df.schema)

tables_today = [row["table_name"] for row in daily_log_df.select("table_name").distinct().collect()]
cleaned_main_log_df = main_log_df.filter(~((col("load_date") == Today_date) & (col("table_name").isin(tables_today))))
final_main_log_df = cleaned_main_log_df.unionByName(daily_log_df)

# Replace Final Log
temp_main_log_path = main_log_path + "_tmp"
final_main_log_df.write.mode("overwrite").parquet(temp_main_log_path)

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
Path = sc._jvm.org.apache.hadoop.fs.Path

main_path = Path(main_log_path)
temp_path = Path(temp_main_log_path)
daily_path = Path(daily_log_path)

if fs.exists(main_path):
    fs.delete(main_path, True)
fs.rename(temp_path, main_path)
if fs.exists(daily_path):
    fs.delete(daily_path, True)

print("Main log updated and daily log cleaned up.")