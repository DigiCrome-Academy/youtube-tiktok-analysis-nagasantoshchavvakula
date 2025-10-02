"""
Script Name: 03_analyze_data.py
Project: YouTube & TikTok Trends Analysis

Purpose:
    This script performs advanced data analysis on the cleaned and processed datasets.
    It extracts meaningful insights, generates summary statistics, and creates
    aggregated datasets and visualizations that can be used for reporting and dashboards.

Workflow:
    1. Connect to the MySQL database (or load cleaned data from `data/processed/`).
    2. Load the processed data into Pandas DataFrames.
    3. Perform advanced analyses such as:
        - Descriptive statistics and correlation analysis
        - Feature engineering (e.g., engagement rate, virality score)
        - Time-series trend analysis
        - Category/creator-based aggregations
        - Comparative analysis between YouTube and TikTok
    4. Save analyzed outputs to `data/analyzed/` 
    4. Generate summary tables and visualizations and save them to `data/visualizations/`

Inputs:
    - Processed datasets from `data/processed/` 
      OR
    - Cleaned tables from MySQL database `youtube_tiktok_trends`

Outputs:
    - Aggregated datasets in `data/outputs/`
    - Statistical summaries and visualizations
    - (Optional) Insights tables stored in MySQL for dashboarding

Dependencies:
    - pandas
    - matplotlib / seaborn (for visualizations)
    - sqlalchemy
    - mysql-connector-python
    - configparser
"""
# ----------------------Importing Libraries---------------------- #
import pandas as pd
import numpy as np
import os
import configparser
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ----------------------connecting to the database---------------------- #
config = configparser.ConfigParser()
config.read('Dashboards/config_cleaned_datasets.ini') # Adjusted path to config_cleaned_datasets.ini
db_config = config['mysql']
DB_HOST = db_config['host']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_NAME = db_config['database']
engine= create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}') #SQLAlchemy engine URL: "mysql+mysqlconnector://<user>:<password>@<host>/<database>"
connection = engine.connect()
print("Connected to the cleaned database successfully.")
print(" ")

# ------------Loading/Fetching cleaned datasets from MySQL tables into Pandas DataFrame---------------------- #
# Query to get all table names in the database
tables_query = text("SHOW TABLES;")
tables_result = connection.execute(tables_query)
table_names = [row[0] for row in tables_result]
print(f"Found tables: {table_names}")
print(" ")

# Load each table into a DataFrame and store in a dictionary
dataframes = {}
for table in table_names:
    query = text(f"SELECT * FROM {table};")
    df = pd.read_sql(query, connection)
    dataframes[table] = df
    print(f"Loaded table '{table}' into DataFrame with shape {df.shape}")
print(" ")

# -------------- Reading above Loaded Dataframes ---------------------- #
# Example: Display first few rows of each DataFrame
for name, df in dataframes.items():
    print(f"DataFrame: {name}")
    print(df.head())
    print(" ")

# -------------- connecting to new database i.e., youtube_tiktok_trends_analyzed ---------------------- #
config = configparser.ConfigParser()
config.read('Dashboards/config_analyzed_datasets.ini') # Adjusted path to config_analyzed_datasets.ini
db_config = config['mysql']
DB_HOST = db_config['host']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_NAME = db_config['database']
engine= create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}') #SQLAlchemy engine URL: "mysql+mysqlconnector://<user>:<password>@<host>/<database>"
connection = engine.connect()
print("Connected to the analyzed database successfully.")
print(" ")

# ------------------------------------------------- Advanced Data Analysis ----------------------------------------------------- #

# 1. Descriptive stats and correlation analysis for each DataFrame
for name, df in dataframes.items():
    print(f"Descriptive statistics for {name}:")
    print(df.describe(include='all'))
    print(" ")
    # select numeric columns for correlation
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 1:
        print(f"Correlation matrix for {name}:")
        print(df[numeric_cols].corr())
        print(" ")
    else:
        print(f"No sufficient numeric columns for correlation analysis in {name}.")
        print(" ")
# Saving analyzed datasets to CSV files in data/analyzed/Descriptive_Correlation_Stats/
output_dir = 'data/analyzed/Descriptive_Correlation_Stats/'
os.makedirs(output_dir, exist_ok=True)
for name, df in dataframes.items():
    desc_stats = df.describe(include='all')
    desc_stats.to_csv(os.path.join(output_dir, f"{name}_descriptive_stats.csv"))
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 1:
        corr_matrix = df[numeric_cols].corr()
        corr_matrix.to_csv(os.path.join(output_dir, f"{name}_correlation_matrix.csv"))
print(f"Saved descriptive statistics and correlation matrices to {output_dir}")
print(" ")

# 2. Feature engineering: Example - Engagement Rate (likes + comments) / views
for name, df in dataframes.items():
    if 'views' in df.columns and 'likes' in df.columns and 'comments' in df.columns:
        df['engagement_rate'] = (df['likes'] + df['comments']) / df['views']
        print(f"Added 'engagement_rate' feature to {name}.")
        # Saving datasets with new features to CSV files in data/analyzed/Feature_Engineering/
        output_dir = 'data/analyzed/Feature_Engineering/'
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(os.path.join(output_dir, f"{name}_with_features.csv"), index=False)
        print(f"Saved dataset with new features to {output_dir}")
    else:
        print(f"Skipping feature engineering for {name} due to missing columns.")
print(" ")

# 3. Time-series trend analysis: Example - Daily average views over time
for name, df in dataframes.items():
    date_col = None
    for col in df.columns:
        if 'date' in col.lower():
            date_col = col
            break
    if date_col and 'views' in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        time_series = df.groupby(df[date_col].dt.date)['views'].mean().reset_index()
        time_series.columns = [date_col, 'average_views']
        print(f"Time-series trend analysis for {name}:")
        print(time_series.head())
        # Saveing time-series trend data to CSV files in data/analyzed/Time_Series_Trends/
        output_dir = 'data/analyzed/Time_Series_Trends/'
        os.makedirs(output_dir, exist_ok=True)
        time_series.to_csv(os.path.join(output_dir, f"{name}_time_series_trend.csv"), index=False)
        print(f"Saved time-series trend data to {output_dir}")
    else:
        print(f"Skipping time-series analysis for {name} due to missing date or views column.")
print(" ")

# 4. Category/creator-based aggregations: Example - Average views and likes per category
for name, df in dataframes.items():
    if 'category' in df.columns and 'views' in df.columns and 'likes' in df.columns:
        category_agg = df.groupby('category').agg({'views': 'mean', 'likes': 'mean'}).reset_index()
        category_agg.columns = ['category', 'average_views', 'average_likes']
        print(f"Category-based aggregation for {name}:")
        print(category_agg.head())
        # Saving category-based aggregation data to CSV files in data/analyzed/Category_Aggregations/
        output_dir = 'data/analyzed/Category_Aggregations/'
        os.makedirs(output_dir, exist_ok=True)
        category_agg.to_csv(os.path.join(output_dir, f"{name}_category_aggregation.csv"), index=False)
        print(f"Saved category-based aggregation data to {output_dir}")
    else:
        print(f"Skipping category-based aggregation for {name} due to missing columns.")
print(" ")

# 5. Comparative analysis between YouTube and TikTok: Example - Average engagement rate
if 'youtube_data' in dataframes and 'tiktok_data' in dataframes:
    yt_df = dataframes['youtube_data']
    tt_df = dataframes['tiktok_data']
    if 'engagement_rate' in yt_df.columns and 'engagement_rate' in tt_df.columns:
        yt_avg_engagement = yt_df['engagement_rate'].mean()
        tt_avg_engagement = tt_df['engagement_rate'].mean()
        comparison_df = pd.DataFrame({
            'Platform': ['YouTube', 'TikTok'],
            'Average_Engagement_Rate': [yt_avg_engagement, tt_avg_engagement]
        })
        print("Comparative analysis of average engagement rate between YouTube and TikTok:")
        print(comparison_df)
        # Saving comparative analysis data to CSV files in data/analyzed/Comparative_Analysis/
        output_dir = 'data/analyzed/Comparative_Analysis/'
        os.makedirs(output_dir, exist_ok=True)
        comparison_df.to_csv(os.path.join(output_dir, "platform_comparative_analysis.csv"), index=False)
        print(f"Saved comparative analysis data to {output_dir}")
    else:
        print("Skipping comparative analysis due to missing 'engagement_rate' in one of the datasets.")
else:
    print("Skipping comparative analysis as one of the datasets (YouTube or TikTok) is missing.")
print(" ")

# 5. Generating summary tables and saving them to data/summary
summary_dir = 'data/analyzed/Summaries/'
os.makedirs(summary_dir, exist_ok=True)
for name, df in dataframes.items():
    summary = {
        'Total Records': len(df),
        'Total Unique Categories': df['category'].nunique() if 'category' in df.columns else 'N/A',
        'Total Unique Creators': df['creator'].nunique() if 'creator' in df.columns else 'N/A',
        'Average Views': df['views'].mean() if 'views' in df.columns else 'N/A',
        'Average Likes': df['likes'].mean() if 'likes' in df.columns else 'N/A',
        'Average Comments': df['comments'].mean() if 'comments' in df.columns else 'N/A',
    }
    summary_df = pd.DataFrame([summary])
    summary_df.to_csv(os.path.join(summary_dir, f"{name}_summary.csv"), index=False)
    print(f"Saved summary for {name} to {summary_dir}")
print(" ")
print("Data analysis complete.")
print(" ")

# ------------------------ Visualization for the analyzed datasets present in data/analyzed/Descriptive_Correlation_Stats/ ------------------ #
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os  

# Directory containing analyzed datasets
analyzed_data_dir = 'data/analyzed/Descriptive_Correlation_Stats/'
#List all files in the directory
csv_files = [f for f in os.listdir(analyzed_data_dir) if f.endswith('.csv')]
print(f"Found {len(csv_files)} CSV files in {analyzed_data_dir}")
print(" ")

# Load and visualize each CSV file
for file in csv_files:
    file_path = os.path.join(analyzed_data_dir, file)
    df = pd.read_csv(file_path)
    print(f"Visualizing data from {file}")
    print(df.head())
    print(" ")

# Detect numeric columns for visualization
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) == 0:
        print(f"No numeric columns found in {file} for visualization.")
        continue
    print(f"Numeric columns in {file}: {numeric_cols.tolist()}")
    print(" ")

# Example visualization: Histograms of numeric columns
output_dir = 'data/Visualizations/Histograms/'
os.makedirs(output_dir, exist_ok=True)
for col in numeric_cols:
    plt.figure(figsize=(10, 6))
    sns.histplot(df[col].dropna(), kde=True)
    plt.title(f'Histogram of {col} in {file}')
    plt.xlabel(col)
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{file.replace('.csv', '')}_{col}_histogram.png"))
    plt.close()
        # print(f"Saved histogram of {col} to {output_dir}")
print(f"Saved histograms to {output_dir}")
print("Histogram visualizations completed.")
print(" ")

# Example visualization: Scatter plot for first two numeric columns
if len(numeric_cols) >= 2:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=df[numeric_cols[0]], y=df[numeric_cols[1]])
        plt.title(f'Scatter Plot of {numeric_cols[0]} vs {numeric_cols[1]} in {file}')
        plt.xlabel(numeric_cols[0])
        plt.ylabel(numeric_cols[1])
        plt.grid(True)
        plt.tight_layout()
        output_dir = 'data/Visualizations/Scatter_Plots/'
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(os.path.join(output_dir, f"{file.replace('.csv', '')}_{numeric_cols[0]}_vs_{numeric_cols[1]}_scatter.png"))
        plt.close()
        print(f"Saved scatter plot to {output_dir}")
else:
        print(f"Not enough numeric columns for scatter plot in {file}.")
        print(" ")
print("Scatter plot visualization completed.")
print(" ")

# Example Correlation heatmap visualizations for all files
for file in csv_files:
    file_path = os.path.join(analyzed_data_dir, file)
    df = pd.read_csv(file_path)
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 1:
        plt.figure(figsize=(12, 8))
        corr = df[numeric_cols].corr()
        sns.heatmap(corr, annot=True, fmt=".2f", cmap='coolwarm', square=True)
        plt.title(f'Correlation Heatmap in {file}')
        plt.tight_layout()
        output_dir = 'data/Visualizations/Correlation_Heatmaps/'
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(os.path.join(output_dir, f"{file.replace('.csv', '')}_correlation_heatmap.png"))
        plt.close()
print(f"Saved correlation heatmap to {output_dir}")
print("Correlation heatmap visualization completed.")




    














































