# -------------------------------------------------------------------
# ### Imports and Initial Setup ###
# This section imports the necessary Python libraries.
# -------------------------------------------------------------------

# os: Imports the os library, which allows the script to interact with the
# operating system, specifically to read environment variables for the database credentials.
import os

# time: Imports the time library, used here to pause the script if the database isn't ready.
import time

# pandas: Imports the powerful pandas library, the primary tool for data manipulation
# and analysis in this script. It's conventionally given the alias `pd`.
import pandas as pd

# sqlalchemy: Imports the `create_engine` function from the SQLAlchemy library. This function
# is used to create a connection "engine" that manages communication with the database.
from sqlalchemy import create_engine

# sqlalchemy.exc: Imports a specific error type, `OperationalError`, which is raised when
# a database connection fails for reasons like the database server not being available.
from sqlalchemy.exc import OperationalError

# Prints a message to the console to indicate that the script has begun running.
print("ETL script started.")


# -------------------------------------------------------------------
# ### Phase 1: EXTRACT ###
# This phase reads the raw data from the CSV files into memory.
# -------------------------------------------------------------------

# These variables store the file paths *inside the Docker container* where the CSV files
# are located. These paths are accessible because the `docker-compose.yml` file mounts
# the local `./data` folder to `/app/data` inside the container.
PATH_READMISSIONS = "/app/data/readmissions.csv"
PATH_HOSPITAL_INFO = "/app/data/hospital_info.csv"

# This is an error-handling block. The code inside the `try` block is executed, but if a
# `FileNotFoundError` occurs (meaning one of the CSV files is missing), the script will
# jump to the `except` block, print an error message, and exit.
try:
    print(f"Reading data from {PATH_READMISSIONS}")
    # pd.read_csv(...) is the core pandas function for reading a CSV file into a DataFrame.
    # `encoding='utf-8'` specifies the character encoding to prevent issues with special characters.
    # `dtype={'Facility ID': str}` tells pandas to treat the "Facility ID" column as a string (text),
    # which prevents it from dropping leading zeros or misinterpreting the IDs as numbers.
    readmissions_df = pd.read_csv(PATH_READMISSIONS, encoding='utf-8', dtype={'Facility ID': str})
    
    print(f"Reading data from {PATH_HOSPITAL_INFO}")
    hospital_info_df = pd.read_csv(PATH_HOSPITAL_INFO, encoding='utf-8', dtype={'Facility ID': str})
    
    print("Data extraction from local files complete.")
except FileNotFoundError as e:
    print(f"Error: Could not find a data file. {e}")
    exit()


# -------------------------------------------------------------------
# ### Phase 2: TRANSFORM ###
# This is the main data cleaning and preparation phase.
# -------------------------------------------------------------------

print("Starting data transformation...")

# --- Step 1: Clean column names ---
# This code cleans the column names in both DataFrames to make them consistent and easier to use.
# It performs a chain of operations on each column name:
# .str.strip():       Removes any leading or trailing whitespace.
# .str.lower():       Converts all characters to lowercase.
# .str.replace(' ', '_'): Replaces spaces with underscores.
# .str.replace('/', '_'): Replaces forward slashes with underscores.
readmissions_df.columns = readmissions_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')
hospital_info_df.columns = hospital_info_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')

# --- Step 2: Convert relevant columns to numeric ---
# This loop converts columns that should contain numbers into a numeric data type.
cols_to_numeric = ['excess_readmission_ratio', 'number_of_discharges']
for col in cols_to_numeric:
    if col in readmissions_df.columns:
        # `pd.to_numeric(...)` attempts to change the column's data type to a number.
        # `errors='coerce'` is very important: If it encounters a value it cannot convert
        # (e.g., text like "N/A"), it will replace that value with `NaN` (Not a Number)
        # instead of stopping with an error.
        readmissions_df[col] = pd.to_numeric(readmissions_df[col], errors='coerce')

# --- Step 3: Drop rows with missing values ---
# This line removes any rows from `readmissions_df` that have a `NaN` value in either
# the `excess_readmission_ratio` or `number_of_discharges` columns. This ensures
# the dataset only contains complete records for these key metrics.
# `inplace=True` modifies the DataFrame directly without needing to reassign it.
readmissions_df.dropna(subset=['excess_readmission_ratio', 'number_of_discharges'], inplace=True)

# --- Step 4: Filter for a single, important measure ---
# This line filters the DataFrame to keep only the rows where the `measure_name` is
# exactly 'READM-30-HF-HRRP' (30-day heart failure readmission rates).
# The `.copy()` is used to create a new, independent DataFrame.
readmissions_filtered = readmissions_df[readmissions_df['measure_name'] == 'READM-30-HF-HRRP'].copy()
print(f"Filtered down to {len(readmissions_filtered)} records for Heart Failure readmissions.")

# --- Step 5: Select columns from hospital info ---
# This creates a smaller, more focused DataFrame containing only the specified columns
# from the original hospital info data.
hospital_info_subset = hospital_info_df[['facility_id', 'facility_name', 'city_town', 'state', 'hospital_type', 'hospital_ownership']]

# --- Step 6 (The Fix): Drop ALL redundant columns from the readmissions data before merging ---
# This is a critical step to prevent data conflicts. Both CSV files contain columns named
# `state` and `facility_name`. To avoid duplicate columns after the merge (e.g., `state_x`, `state_y`),
# this line removes them from the readmissions data, establishing the hospital_info data as the
# single source of truth for these fields.
readmissions_filtered = readmissions_filtered.drop(columns=['state', 'facility_name'])

# --- Step 7: Merge the two dataframes ---
# This line combines the two prepared DataFrames into a single `final_df`.
# `on='facility_id'` specifies that the join should match rows based on the `facility_id` column.
# `how='inner'` means that only rows with a matching `facility_id` in *both* DataFrames
# will be included in the final result.
final_df = pd.merge(readmissions_filtered, hospital_info_subset, on='facility_id', how='inner')
print(f"Data transformation complete. Final dataset has {len(final_df)} records.")


# -------------------------------------------------------------------
# ### Phase 3: LOAD ###
# This final phase connects to the PostgreSQL database and writes
# the transformed data into a table.
# -------------------------------------------------------------------

print("Starting data load to PostgreSQL...")

# Retrieve the database connection details from the environment variables
# set in the `docker-compose.yml` file.
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')

# Construct the full database connection string (URL) in the format that SQLAlchemy expects.
db_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}'

# This `while` loop is a robustness mechanism. It tries to connect to the database.
# If it fails (likely because the ETL container started faster than the database container),
# it will wait for 10 seconds and try again, up to 5 times.
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

# If the loop finishes without successfully connecting, the script prints an error and exits.
if not connected:
    print("Could not connect to the database. Exiting.")
    exit()

# This `try...except...finally` block handles the actual data loading.
try:
    table_name = 'heart_failure_readmissions'
    # `final_df.to_sql(...)` writes the entire DataFrame to a SQL table.
    # `if_exists='replace'`: If the table already exists, it will be dropped and recreated.
    # `index=False`: Prevents pandas from writing the DataFrame's row index as a SQL column.
    final_df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Successfully loaded {len(final_df)} rows into the '{table_name}' table.")
except Exception as e:
    # Catches any potential errors during the `to_sql` operation and prints them.
    print(f"Error loading data to PostgreSQL: {e}")
finally:
    # This `finally` block is ALWAYS executed, whether an error occurred or not.
    # `connection.close()` ensures the database connection is properly closed.
    connection.close()
    print("Database connection closed.")

print("ETL process finished.")