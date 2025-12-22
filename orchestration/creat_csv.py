# importing neccessary libraries
import duckdb
import os

# 1. Setup Paths
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, "data", "final", "global_connectivity.duckdb")
CSV_PATH = os.path.join(BASE_DIR, "data", "final", "dashboard_data.csv")

# Ensure the database exists or not
if not os.path.exists(DB_PATH):
    print(f"❌ Error: Database not found at {DB_PATH}")
    exit(1)

print(f"🔌 Connecting to database at: {DB_PATH}")
con = duckdb.connect(DB_PATH)

# 2. Find the correct table name
existing_tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
print(f"📋 Found tables in DB: {existing_tables}")

# Logic to pick the valid table
if "internet_speeds" in existing_tables:
    table_name = "internet_speeds"
elif "global_internet_speeds" in existing_tables:
    table_name = "global_internet_speeds"
elif "df_final_clean" in existing_tables:  # <--- Added this line
    table_name = "df_final_clean"
else:
    print("❌ Error: Could not find any valid speed data table.")
    print(f"Available tables: {existing_tables}")
    exit(1)

# 3. Export to CSV file format to use dashboard integrity
print(f"💾 Exporting table '{table_name}' to CSV...")
con.execute(f"COPY {table_name} TO '{CSV_PATH}' (HEADER, DELIMITER ',')")

print(f"✅ Success! File created at: {CSV_PATH}")
con.close()
