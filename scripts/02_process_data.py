"""
Script Name: 02_process_data.py
Project: YouTube & TikTok Trends Analysis

Purpose:
This script processes raw data stored in the MySQL database and prepares cleaned,
analysis-ready datasets. It performs data cleaning, transformation, and feature
engineering before saving the results into the `data/processed/` directory.

Workflow:
1. Connect to the MySQL database using credentials from config.ini.
2. Read data from database tables into Pandas DataFrames.
3. Perform data cleaning:
- Handle missing values
- Correct data types
- Remove duplicates
4. Perform feature engineering:
- Calculate engagement rates
- Compute views per creator and other derived metrics
5. Save cleaned DataFrames as CSV files in the `data/processed/` directory.
6. Version cleaned data with DVC for reproducibility.

Inputs:
- MySQL database tables populated with raw YouTube & TikTok trend data

Outputs:
- Cleaned CSV files stored in `data/processed/`
- DVC-tracked processed data files

Dependencies:
- pandas
- sqlalchemy
- mysql-connector-python
- configparser
- dvc
"""
import os
import pandas as pd
from sqlalchemy import create_engine
import configparser

# ----------------------Connecting to MySQL Database---------------------- #
config = configparser.ConfigParser()
config.read('Dashboards/config.ini') # Adjusted path to config.ini
db_config = config['mysql']
DB_HOST = db_config['host']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_NAME = db_config['database']

# ----------------------Database Connection---------------------- #
engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}') #SQLAlchemy engine URL: "mysql+mysqlconnector://<user>:<password>@<host>/<database>"
connection = engine.connect()
print("Connected to the database successfully.")
print(" ")

# ---------- Get list of tables in the database ----------------------- #
tables = pd.read_sql("SHOW TABLES", connection)
table_list = tables.iloc[:,0].tolist() # List of table names
print(f"Found tables: {table_list}")
print(" ")

# ----------------------Read data from MySQL tables into Pandas DataFrames---------------------- #
def read_table_to_df(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection)
    print(f"Read table {table_name} into DataFrame with shape {df.shape}")
    return df
dataframes = {table: read_table_to_df(table) for table in table_list}
print(" ")

# ----------------------Data Cleaning --`---------------------- #
def clean_dataframe(df):
    # Handle missing values
    df=df.dropna(how='all') # Drop rows where all elements are missing 
    df=df.fillna(0) # Fill remaining NaNs with 0

    # Remove duplicates
    df=df.drop_duplicates() # Remove duplicate rows
    df=df.reset_index(drop=True) # Reset index after dropping duplicates
    return df
    
    # Correct data types (example: convert date columns to datetime)
def correct_data_types(df):
    for col in df.select_dtypes(include=['object']).columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce') # Convert to datetime, coerce errors to NaT
    return df

# -------Tables after cleaning-------- #
cleaned_dataframes = {}
for table, df in dataframes.items():
    df = clean_dataframe(df)
    df = correct_data_types(df)
    cleaned_dataframes[table] = df
    print(f"Cleaned DataFrame for table {table} now has shape {df.shape}")
print(" ")

# ----------------------Feature Engineering---------------------- #
def feature_engineering(df, table_name):
    if 'youtube' in table_name:
        if 'views' in df.columns and 'subscribers' in df.columns:
            df['views_per_subscriber'] = df['views'] / df['subscribers'].replace(0, 1) # Avoid division by zero
        if 'likes' in df.columns and 'views' in df.columns:
            df['like_rate'] = df['likes'] / df['views'].replace(0, 1) # Avoid division by zero
    elif 'tiktok' in table_name:
        if 'likes' in df.columns and 'followers' in df.columns:
            df['likes_per_follower'] = df['likes'] / df['followers'].replace(0, 1) # Avoid division by zero
        if 'shares' in df.columns and 'followers' in df.columns:
            df['shares_per_follower'] = df['shares'] / df['followers'].replace(0, 1) # Avoid division by zero
    return df
engineered_dataframes = {}

# Calculate new metrics like engagement rates and views per creator
for table, df in cleaned_dataframes.items():
    df = feature_engineering(df, table)
    engineered_dataframes[table] = df
    print(f"Engineered features for table {table}. DataFrame now has shape {df.shape}")
print(" ")

# ----------------------Save cleaned DataFrames as CSV files in data/processed/---------------------- #
processed_dir = 'data/processed/'
os.makedirs(processed_dir, exist_ok=True) # Create directory if it doesn't exist
for table, df in engineered_dataframes.items():
    file_path = os.path.join(processed_dir, f"{table}_cleaned.csv")
    df.to_csv(file_path, index=False)
    print(f"Saved cleaned DataFrame for table {table} to {file_path}")
print(" ")

# ----------------------Close the database connection---------------------- #
engine.dispose()
print("Database connection closed.")


