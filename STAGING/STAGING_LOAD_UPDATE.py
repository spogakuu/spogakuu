from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz
import threading
import traceback

# Initialize Spark
spark = SparkSession.builder.appName("FINAL Update Logic").getOrCreate()

# Setup timezone
desired_timezone = pytz.timezone("US/Central")
Today_date = datetime.now(desired_timezone).strftime("%Y-%m-%d")

# Paths
full_load_path = "Files/Bronze/STG/DSS_INC_FULL_LOAD_HISTORY_PM"
staging_path = "Files/Bronze/STG"
target_path = "Files/Bronze/TST"
object_list_path = "Files/Bronze/COPY_TABLES/POC_TABLE_LIST"
daily_log_path = "Files/Bronze/LOGS/STAGING_DAILY_LOGS"
main_log_path = "Files/Bronze/LOGS/STAGING_MAIN_LOGS"

# Load metadata
object_df = spark.read.parquet(object_list_path)
active_df = object_df.filter("status = 'A'")
full_load_df = spark.read.parquet(full_load_path).cache()

# Load max timestamp
try:
    max_ts = spark.read.parquet("Files/Bronze/STG/FULL_LOAD_HIST_MAX_TIMESTAMP").collect()[0]['MAX_TIMESTAMP']
    print(f"✅ max_ts loaded: {max_ts}")
except Exception as e:
    print("❌ Could not load max_ts. Exiting...")
    raise e

# Filesystem helpers
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

# Thread-safe logging
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

# Core Update Logic
def process_table_update(row_dict):
    table_name = row_dict["OBJECT_NAME"]
    key_col = row_dict["KEY_COLUMN"].split(',')[0]
    table_name_lower = table_name.lower()
    start_time = datetime.now()

    try:
        stg_folder = get_actual_folder(stg_folders, table_name)
        tst_folder = get_actual_folder(tst_folders, table_name)

        stg_df = spark.read.parquet(f"{staging_path}/{stg_folder}")
        try:
            target_df = spark.read.parquet(f"{target_path}/{tst_folder}")
        except:
            target_df = spark.createDataFrame([], stg_df.schema)

        # Filter Full Load
        filtered_full_df = full_load_df.filter(
            (col("UTCTimeStamp") > max_ts) &
            (col("type") == "Insert") &
            (col("table_name") == table_name_lower)
        )

        # Join full_load with staging
        join_df = filtered_full_df.join(
            stg_df, filtered_full_df["row_number"] == stg_df[key_col], "inner"
        )

        # Select only staging columns after join
        columns_to_keep = stg_df.columns
        join_df = join_df.select(columns_to_keep).dropDuplicates()

        # ✅ CLEAN the column names to remove any alias reference
        join_df = join_df.selectExpr(*[f"`{c}`" for c in join_df.columns])

        # Now slim target
        target_slim_df = target_df.select(key_col)

        # Safe join
        update_df = join_df.join(
            target_slim_df, join_df[key_col] == target_slim_df[key_col], "inner"
        )

        # Drop existing 'status' or 'insert_timestamp'
        for col_to_add in ["status", "insert_timestamp"]:
            if col_to_add in update_df.columns:
                update_df = update_df.drop(col_to_add)

        # Add new columns
        update_df = update_df.withColumn("status", lit('U')).withColumn("insert_timestamp", current_timestamp())

        # Save
        update_df.write.mode("overwrite").parquet(f"Files/Bronze/NEW_VIEWS/{table_name_lower}_UPDATE")

        end_time = datetime.now()
        print(f"✅ UPDATE SUCCESS: {table_name}")
        update_log(table_name + '_UPDATE', "SUCCESS", start_time, end_time)

    except Exception as e:
        end_time = datetime.now()
        error_message = traceback.format_exc()
        print(f"❌ UPDATE FAILURE for {table_name}: {e}")
        update_log(table_name + '_UPDATE', "FAILURE", start_time, end_time, error_message)

# Select tables
try:
    main_log_df = spark.read.parquet(main_log_path)
    failed_today_df = main_log_df.filter(f"load_date = '{Today_date}' AND status = 'FAILURE'")
    failed_today_tables = [row["table_name"] for row in failed_today_df.collect()]
    main_logged_today_tables = main_log_df.filter(f"load_date = '{Today_date}'") \
        .select("table_name").distinct().rdd.flatMap(lambda x: x).collect()

    if failed_today_tables:
        tables_to_run = [item for item in active_df.collect() if item["OBJECT_NAME"] + '_UPDATE' in failed_today_tables]
        print(f"Retrying failed tables: {[row['OBJECT_NAME'] for row in tables_to_run]}")
    elif set([x["OBJECT_NAME"] + '_UPDATE' for x in active_df.collect()]).issubset(set(main_logged_today_tables)):
        tables_to_run = []
        print("All tables already processed today.")
    else:
        tables_to_run = active_df.collect()
        print("Running all active tables first time.")
except Exception as e:
    print("No main log found — running all tables.")
    tables_to_run = active_df.collect()

# Execute in parallel
if tables_to_run:
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_table_update, row.asDict()) for row in tables_to_run]
        for future in as_completed(futures):
            future.result()

# Merge daily into main log
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

# Save
temp_main_log_path = main_log_path + "_tmp"
final_main_log_df.write.mode("overwrite").parquet(temp_main_log_path)

main_path = Path(main_log_path)
temp_path = Path(temp_main_log_path)
daily_path = Path(daily_log_path)

if fs.exists(main_path):
    fs.delete(main_path, True)
fs.rename(temp_path, main_path)
if fs.exists(daily_path):
    fs.delete(daily_path, True)

print("✅ FINAL Update Process completed successfully — No Duplicate Columns, No Ambiguous References!")