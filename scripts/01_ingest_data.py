"""
Script Name: 01_ingest_data.py
Project: YouTube & TikTok Trends Analysis

Purpose:
    This script ingests raw CSV data from the `data/raw/` directory into a MySQL database.
    It reads each CSV file into a Pandas DataFrame, creates corresponding tables in MySQL,
    and loads the data using Pandas' `to_sql()` method.

Workflow:
    1. Connect to the MySQL database using credentials from config.ini.
    2. Iterate over all CSV files in the `data/raw/` directory.
    3. Read each CSV file into a Pandas DataFrame.
    4. Automatically create tables in MySQL based on DataFrame schema.
    5. Load data from DataFrames into corresponding MySQL tables.

Inputs:
    - CSV files located in `data/raw/`

Outputs:
    - Tables created in MySQL database `youtube_tiktok_trends`
    - Data loaded from CSV into corresponding tables

Dependencies:
    - pandas
    - sqlalchemy
    - mysql-connector-python
    - configparser
"""
# ----------------------Importing Libraries---------------------- #
import os
import pandas as pd
from sqlalchemy import create_engine
import configparser

# ----------------------Configuration Setup---------------------- #
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

# ----------------------Read each CSV files from data/raw into Pandas DataFrame ----------------------- #
data_dir = 'data/raw/'
csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')] # List of CSV files in the directory
for file in csv_files:
    file_path = os.path.join(data_dir, file) # Full path to the CSV file 
    df = pd.read_csv(file_path) # Read CSV into DataFrame
    print(f"Read {file} into DataFrame with shape {df.shape}") # Print read status and shape of DataFrame

# ----------------------Create table in MySQL and load data from DataFrame into MySQL table----------------------- #
    table_name = os.path.splitext(file)[0].lower() # Table name derived from file name in lowercase for consistency
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False) # Load DataFrame into MySQL table
    print(f"Loaded data into table '{table_name}' in the database.")
    print(" ")
# ----------------------Close the database connection---------------------- #
connection.close()
print("Database connection closed.")
