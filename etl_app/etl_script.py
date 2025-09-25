import os
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

print("ETL script started.")

# --- EXTRACT (from local files) ---
PATH_READMISSIONS = "/app/data/readmissions.csv"
PATH_HOSPITAL_INFO = "/app/data/hospital_info.csv"

try:
    print(f"Reading data from {PATH_READMISSIONS}")
    readmissions_df = pd.read_csv(PATH_READMISSIONS, encoding='utf-8', dtype={'Facility ID': str})
    print(f"Reading data from {PATH_HOSPITAL_INFO}")
    hospital_info_df = pd.read_csv(PATH_HOSPITAL_INFO, encoding='utf-8', dtype={'Facility ID': str})
    print("Data extraction from local files complete.")
except FileNotFoundError as e:
    print(f"Error: Could not find a data file. {e}")
    exit()

# --- TRANSFORM ---
print("Starting data transformation...")

# 1. Clean column names
readmissions_df.columns = readmissions_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')
hospital_info_df.columns = hospital_info_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')

# 2. Convert relevant columns to numeric
cols_to_numeric = ['excess_readmission_ratio', 'number_of_discharges']
for col in cols_to_numeric:
    if col in readmissions_df.columns:
        readmissions_df[col] = pd.to_numeric(readmissions_df[col], errors='coerce')

# 3. Drop rows with missing values
readmissions_df.dropna(subset=['excess_readmission_ratio', 'number_of_discharges'], inplace=True)

# 4. Filter for a single, important measure
readmissions_filtered = readmissions_df[readmissions_df['measure_name'] == 'READM-30-HF-HRRP'].copy()
print(f"Filtered down to {len(readmissions_filtered)} records for Heart Failure readmissions.")

# 5. Select columns from hospital info
hospital_info_subset = hospital_info_df[['facility_id', 'facility_name', 'city_town', 'state', 'hospital_type', 'hospital_ownership']]

# ‼️ --- FIX: Drop ALL redundant columns from the readmissions data before merging ---
readmissions_filtered = readmissions_filtered.drop(columns=['state', 'facility_name'])
# -----------------------------------------------------------------------------------------

# 6. Merge the two dataframes
final_df = pd.merge(readmissions_filtered, hospital_info_subset, on='facility_id', how='inner')
print(f"Data transformation complete. Final dataset has {len(final_df)} records.")


# --- LOAD ---
# The LOAD section is unchanged and remains correct.
print("Starting data load to PostgreSQL...")
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}'

connected = False
retries = 5
while not connected and retries > 0:
    try:
        engine = create_engine(db_url)
        connection = engine.connect()
        connected = True
        print("Successfully connected to the database.")
    except OperationalError:
        print(f"Database not ready, waiting... ({retries} retries left)")
        retries -= 1
        time.sleep(10)

if not connected:
    print("Could not connect to the database. Exiting.")
    exit()

try:
    table_name = 'heart_failure_readmissions'
    final_df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Successfully loaded {len(final_df)} rows into the '{table_name}' table.")
except Exception as e:
    print(f"Error loading data to PostgreSQL: {e}")
finally:
    connection.close()
    print("Database connection closed.")

print("ETL process finished.")