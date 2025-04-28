from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, current_timestamp
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz
import threading
import traceback

# Initialize Spark
spark = SparkSession.builder.appName("Update Logic Full Fabric").getOrCreate()

# Setup timezone
desired_timezone = pytz.timezone("US/Central")
Today_date = datetime.now(desired_timezone).strftime("%Y-%m-%d")

# Paths
full_load_path = "Files/Bronze/STG/DSS_INC_FULL_LOAD_HISTORY_PM"
staging_path = "Files/Bronze/STG"
target_path = "Files/Bronze/TST"
object_list_path = "Files/Bronze/COPY_TABLES/POC_TABLE_LIST"

# ✅ Correct Log Paths
daily_log_path = "Files/Bronze/LOGS/STAGING_DAILY_LOGS"
main_log_path = "Files/Bronze/LOGS/STAGING_MAIN_LOGS"

# Load static datasets
object_df = spark.read.parquet(object_list_path)
active_df = object_df.filter("status = 'A'")
full_load_df = spark.read.parquet(full_load_path).cache()

# Load max_ts
try:
    max_ts = spark.read.parquet("Files/Bronze/STG/FULL_LOAD_HIST_MAX_TIMESTAMP").collect()[0]['MAX_TIMESTAMP']
    print(f"✅ max_ts loaded: {max_ts}")
except Exception as e:
    print("❌ Could not load max_ts. Exiting...")
    raise e

# Native Fabric-friendly folder listing
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
Path = sc._jvm.org.apache.hadoop.fs.Path

def list_folders(base_path):
    folders = []
    base = Path(base_path)
    if fs.exists(base):
        status = fs.listStatus(base)
        for fileStatus in status:
            if fileStatus.isDirectory():
                folders.append(fileStatus.getPath().getName())
    return folders

stg_folders = list_folders(staging_path)
tst_folders = list_folders(target_path)

# Thread-safe log writing
log_write_lock = threading.Lock()

def get_actual_folder(base_list, table_name):
    for folder in base_list:
        if folder.lower() == table_name.lower():
            return folder
    raise Exception(f"Folder not found for {table_name}")

def ensure_log_schema():
    try:
        schema = spark.read.parquet(daily_log_path).schema
    except:
        try:
            main_log_df = spark.read.parquet(main_log_path)
            schema = main_log_df.schema
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.mode("overwrite").parquet(daily_log_path)
        except Exception as ex:
            print("Error initializing logs:", ex)
            raise ex
    return schema

def update_log(table_name, status, start_time, end_time, error_message=""):
    schema = ensure_log_schema()
    new_row = [(table_name, status, Today_date, start_time.isoformat(), end_time.isoformat(), error_message)]
    new_df = spark.createDataFrame(new_row, schema)

    with log_write_lock:
        new_df.write.mode("append").parquet(daily_log_path)
    print(f"Log updated for {table_name} - {status}")

# Update logic per table (💥 aliasing clean version + _UPDATE in log)
def process_table(row_dict):
    table_name = row_dict["OBJECT_NAME"]
    key_col = row_dict["KEY_COLUMN"].split(',')[0]
    table_name_lower = table_name.lower()
    start_time = datetime.now()

    try:
        stg_folder = get_actual_folder(stg_folders, table_name)
        tst_folder = get_actual_folder(tst_folders, table_name)

        stg_df = spark.read.parquet(f"{staging_path}/{stg_folder}").alias("stg")
        try:
            target_df = spark.read.parquet(f"{target_path}/{tst_folder}").alias("tgt")
        except:
            target_df = spark.createDataFrame([], stg_df.schema).alias("tgt")

        filtered_full_df = full_load_df.filter(
            (col("UTCTimestamp") > max_ts) &
            (col("type") == "Insert") &    
            (col("table_name") == table_name_lower)
        ).alias("full")

        join_df = filtered_full_df.join(
            stg_df, col("full.row_number") == col(f"stg.{key_col}"), "inner"
        ).select("stg.*").dropDuplicates()

        update_df = join_df.alias("new").join(
            target_df.alias("existing"), col(f"new.{key_col}") == col(f"existing.{key_col}"), "inner"
        ).select("new.*")

        update_df.write.mode("overwrite").parquet(f"Files/Bronze/NEW_VIEWS/{table_name_lower}_UPDATE")

        end_time = datetime.now()
        print(f"SUCCESS: {table_name}")
        update_log(table_name + "_UPDATE", "SUCCESS", start_time, end_time)

    except Exception as e:
        end_time = datetime.now()
        error_message = traceback.format_exc()
        print(f"FAILURE for {table_name}: {e}")
        update_log(table_name + "_UPDATE", "FAILURE", start_time, end_time, error_message)

# ✅ Smarter decide which tables to run for UPDATE
try:
    main_log_df = spark.read.parquet(main_log_path)
    failed_today_df = main_log_df.filter(f"load_date = '{Today_date}' AND status = 'FAILURE'")
    failed_today_tables = [row["table_name"] for row in failed_today_df.collect()]
    main_logged_today_tables = main_log_df.filter(f"load_date = '{Today_date}'") \
        .select("table_name").distinct().rdd.flatMap(lambda x: x).collect()

    all_active_tables = [item for item in active_df.collect()]

    if failed_today_tables:
        tables_to_run = [item for item in all_active_tables if (item["OBJECT_NAME"] + "_UPDATE") in failed_today_tables]
        print(f"Retrying failed UPDATE tables only: {[row['OBJECT_NAME'] for row in tables_to_run]}")
    else:
        processed_tables_today = set(main_logged_today_tables)
        tables_to_run = [item for item in all_active_tables if (item["OBJECT_NAME"] + "_UPDATE") not in processed_tables_today]
        print(f"Running only pending UPDATE tables: {[row['OBJECT_NAME'] for row in tables_to_run]}")

except Exception as e:
    print("No main log found — running all tables.")
    tables_to_run = active_df.collect()

# Execute in parallel
if tables_to_run:
    with ThreadPoolExecutor(max_workers=17) as executor:
        futures = [executor.submit(process_table, row.asDict()) for row in tables_to_run]
        for future in as_completed(futures):
            future.result()

# Merge daily log into main log
try:
    main_log_df = spark.read.parquet(main_log_path)
    spark.catalog.refreshByPath(daily_log_path)
    daily_log_df = spark.read.parquet(daily_log_path)
except Exception as e:
    print("Daily log missing, using empty.")
    daily_log_df = spark.createDataFrame([], main_log_df.schema)

tables_today = [row["table_name"] for row in daily_log_df.select("table_name").distinct().collect()]
cleaned_main_log_df = main_log_df.filter(~((col("load_date") == Today_date) & (col("table_name").isin(tables_today))))
final_main_log_df = cleaned_main_log_df.unionByName(daily_log_df)

temp_main_log_path = main_log_path + "_tmp"
final_main_log_df.write.mode("overwrite").parquet(temp_main_log_path)

if fs.exists(Path(main_log_path)):
    fs.delete(Path(main_log_path), True)
fs.rename(Path(temp_main_log_path), Path(main_log_path))
if fs.exists(Path(daily_log_path)):
    fs.delete(Path(daily_log_path), True)

print("✅ Main log updated and daily log cleaned.")

# Recreate daily log if missing
try:
    if not fs.exists(Path(daily_log_path)):
        print("Daily log missing. Recreating daily log from main log schema...")
        main_log_df = spark.read.parquet(main_log_path)
        schema = main_log_df.schema
        empty_daily_log_df = spark.createDataFrame([], schema)
        empty_daily_log_df.write.mode("overwrite").parquet(daily_log_path)
        print("✅ Daily log created successfully.")
    else:
        print("✅ Daily log already exists.")
except Exception as final_error:
    print("❌ Final step error while checking/creating daily log:", final_error)

print("✅ Update process with smart skip/retry completed successfully!")