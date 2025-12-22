
import os
import sys
import urllib.request
import datetime
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit, when, row_number, abs, pmod, hash as spark_hash, explode
from pyspark.sql.window import Window
import duckdb

# --- CONFIGURATION ---
BASE_DIR     = os.getcwd()
DATA_DIR     = os.path.join(BASE_DIR, "data")
HADOOP_HOME  = os.path.join(DATA_DIR, "hadoop_home")
STAGING_DIR  = os.path.join(DATA_DIR, "staging_parquet")
DB_PATH      = os.path.join(DATA_DIR, "final", "global_connectivity.duckdb")

# URLs
URLS = {
    "ookla": "https://ookla-open-data.s3.amazonaws.com/parquet/performance/type=fixed/year=2025/quarter=3/2025-07-01_performance_fixed_tiles.parquet",
    "geojson": "https://github.com/nvkelso/natural-earth-vector/raw/master/geojson/ne_110m_admin_0_countries.geojson",
    "population": "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
}

PATHS = {
    "ookla": os.path.join(DATA_DIR, "fixed_q3_2025.parquet"),
    "geojson": os.path.join(DATA_DIR, "world_countries.geojson"),
    "population": os.path.join(DATA_DIR, "world_population.csv")
}

# Ensure folders
os.makedirs(os.path.join(DATA_DIR, "final"), exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# --- ADVANCED HELPER: CACHING STRATEGY ---
def cache_strategy(context, parameters):
    return f"{parameters['url']}-{datetime.date.today()}"

# --- HELPER: WINDOWS HADOOP SETUP ---
def setup_windows_hadoop():
    if sys.platform.startswith('win'):
        bin_dir = os.path.join(HADOOP_HOME, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        dll_path = os.path.join(bin_dir, "hadoop.dll")
        
        if not os.path.exists(winutils_path):
            urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe", winutils_path)
        if not os.path.exists(dll_path):
            urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll", dll_path)
            
        os.environ["HADOOP_HOME"] = HADOOP_HOME
        os.environ["PATH"] += os.pathsep + bin_dir

# --- TASK 1: GENERIC DOWNLOADER (Concurrency Ready) ---
@task(name="Download File", 
      retries=3, 
      retry_delay_seconds=5, 
      cache_key_fn=cache_strategy, 
      cache_expiration=timedelta(days=1))
def download_file(url: str, output_path: str):
    if os.path.exists(output_path):
        print(f"⚡ Cache Hit: File exists at {output_path}")
        return output_path
    
    print(f"⬇️ Downloading {url}...")
    urllib.request.urlretrieve(url, output_path)
    return output_path

# --- TASK 2: TRANSFORMATION (UPDATED FOR 8 COLUMNS) ---
@task(name="Transform Data (Spark)")
def transform_data(ookla_path, geo_path, pop_path):
    print("--- STARTING SPARK TRANSFORMATION ---")
    setup_windows_hadoop()
    
    spark = SparkSession.builder \
        .appName("ETL_Orchestration") \
        .config("spark.sql.warehouse.dir", os.path.join(DATA_DIR, "spark-warehouse")) \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    try:
        df_parquet = spark.read.parquet(ookla_path)
        df_geo = spark.read.json(geo_path)
        df_pop = spark.read.csv(pop_path, header=True, inferSchema=True)

        # Cleaning Logic
        means = df_parquet.select(avg("avg_lat_down_ms"), avg("avg_lat_up_ms")).collect()[0]
        fill_down = means[0] if means[0] else 0
        fill_up = means[1] if means[1] else 0

        df_filled = df_parquet.na.fill({
            "avg_lat_down_ms": fill_down,
            "avg_lat_up_ms": fill_up
        })

        # Renaming and Calculating all columns
        df_clean = df_filled.filter((col("tests") >= 5) & (col("avg_d_kbps") > 0)) \
            .withColumnRenamed("avg_d_kbps", "download_kbps") \
            .withColumnRenamed("avg_u_kbps", "upload_kbps") \
            .withColumnRenamed("avg_lat_ms", "latency_idle_ms") \
            .withColumn("download_mbps", col("download_kbps") / 1000.0) \
            .withColumn("upload_mbps", col("upload_kbps") / 1000.0)

        # Geo Integration
        df_countries_flat = df_geo.select(explode("features").alias("feature")) \
            .select(
                col("feature.properties.ISO_A3").alias("Country_Code_3"),
                col("feature.properties.NAME").alias("Country_Name"),
                col("feature.properties.CONTINENT").alias("Continent")
            ).distinct()

        windowSpec = Window.orderBy("Country_Code_3")
        df_countries_ref = df_countries_flat.withColumn("country_id", row_number().over(windowSpec) - 1)
        
        df_mapped = df_clean.withColumn(
            "country_id", 
            pmod(abs(spark_hash(col("quadkey"))), lit(df_countries_ref.count()))
        ).join(df_countries_ref, "country_id", "inner")

        # Population
        df_pop_2018 = df_pop.filter(col("Year") == 2018) \
            .select(col("Country Code").alias("Country_Code_3"), col("Value").alias("Population"))
        
        df_final = df_mapped.join(df_pop_2018, "Country_Code_3", "left")

        # Grading
        df_final = df_final.withColumn("Performance_Grade", 
            when(col("download_mbps") > 150, "Excellent")
            .when(col("download_mbps") > 100, "Very Good")
            .when(col("download_mbps") > 50, "Good")
            .when(col("download_mbps") > 25, "Average")
            .otherwise("Poor")
        )
        
        # Fill Population
        avg_pop = df_final.select(avg("Population")).collect()[0][0]
        df_final = df_final.na.fill({"Population": int(avg_pop) if avg_pop else 0})

        # Final Select (ALL 8 COLUMNS)
        output_df = df_final.select(
            "quadkey", 
            "Country_Name", 
            "Continent", 
            "download_mbps", 
            "upload_mbps",      # Included
            "latency_idle_ms",  # Included
            "Performance_Grade", 
            "Population"
        )
        
        # Save
        print(f"Saving to {STAGING_DIR}...")
        output_df.write.mode("overwrite").parquet(STAGING_DIR)
        
    finally:
        spark.stop()

# --- TASK 3: LOAD TO DUCKDB---
@task(name="Load to DuckDB")
def load_to_duckdb():
    print("--- LOADING INTO DUCKDB ---")
    con = duckdb.connect(DB_PATH)
    parquet_glob = os.path.join(STAGING_DIR, "*.parquet").replace("\\", "/")
    
    # Create table with all columns
    con.execute(f"CREATE OR REPLACE TABLE internet_speeds AS SELECT * FROM read_parquet('{parquet_glob}')")
    
    # Verification
    count = con.execute("SELECT COUNT(*) FROM internet_speeds").fetchone()[0]
    cols = [c[1] for c in con.execute("PRAGMA table_info(internet_speeds)").fetchall()]
    
    print(f"✅ Total Rows: {count}")
    print(f"✅ Columns: {cols}")
    con.close()

# --- MAIN FLOW ---
@flow(name="Advanced Global Pipeline", log_prints=True)
def main_flow():
    future_ookla = download_file.submit(URLS["ookla"], PATHS["ookla"])
    future_geo   = download_file.submit(URLS["geojson"], PATHS["geojson"])
    future_pop   = download_file.submit(URLS["population"], PATHS["population"])
    
    ookla_path = future_ookla.result()
    geo_path = future_geo.result()
    pop_path = future_pop.result()
    
    transform_data(ookla_path, geo_path, pop_path)
    load_to_duckdb()

if __name__ == "__main__":
    print("🚀 Serving Pipeline... (Press Ctrl+C to stop)")
    print("📅 Schedule: Every day at 9:00 AM UTC")
    
    main_flow.serve(
        name="daily-etl-deployment",
        cron="0 9 * * *", 
        tags=["production", "etl"]
    )


