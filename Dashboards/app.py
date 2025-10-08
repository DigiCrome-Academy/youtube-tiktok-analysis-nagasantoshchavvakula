"""
Script Name: app.py
Project: YouTube & TikTok Trends Dashboard

Purpose:
    This script builds an interactive Streamlit dashboard for visualizing YouTube and TikTok trends.
    It connects to the MySQL database containing processed social media data, loads the datasets
    into Pandas DataFrames, and generates a series of interactive and static visualizations to 
    analyze engagement metrics such as views, likes, dislikes, and comments.

Workflow:
    1. Load MySQL configuration details from `Dashboards/config_cleaned_datasets.ini`.
    2. Establish a database connection using SQLAlchemy.
    3. Automatically read all CSV files from `data/processed/` and load them into MySQL tables.
    4. Retrieve a specific table from MySQL into a Pandas DataFrame for visualization.
    5. Launch a Streamlit dashboard that:
        - Displays platform and category-based filters in the sidebar.
        - Shows the filtered dataset dynamically.
        - Renders key visualizations:
            a. Total Views by Platform (Plotly)
            b. Likes vs Dislikes by Platform (Matplotlib)
            c. Comments Distribution by Platform (Seaborn boxplot)
    6. Apply a consistent corporate visualization theme using Seaborn and Matplotlib styling.

Inputs:
    - MySQL database tables (created from `data/processed/` CSVs)
    - Configuration file: `Dashboards/config_cleaned_datasets.ini`
        [mysql]
        host = <database_host>
        user = <username>
        password = <password>
        database = <database_name>

Outputs:
    - Streamlit web application displaying:
        • Interactive data filters
        • Aggregated analytics visualizations
        • Dynamic data tables for exploration

Dependencies:
    - pandas
    - sqlalchemy
    - mysql-connector-python
    - configparser
    - matplotlib
    - seaborn
    - plotly
    - streamlit
    - os

Usage:
    Run the following command from the terminal:
        streamlit run app.py

    This will launch the dashboard locally, accessible via a browser at:
        http://localhost:8501

Notes:
    - Ensure MySQL service is running and accessible before launching the dashboard.
    - The config file must contain valid credentials and the database should exist.
    - All processed CSVs in `data/processed/` will be automatically uploaded to MySQL tables.
"""

# ----------------------Importing Libraries---------------------- #
import os
import pandas as pd
from sqlalchemy import create_engine
import configparser
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

# ----------------------Configuration Setup---------------------- #
config = configparser.ConfigParser()
config.read('Dashboards/config_cleaned_datasets.ini') # Adjusted path to config.ini
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
# ---------- Loading datasets from data/processed to MySQL database ----------------------- #
data_dir = 'data/processed/'
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
    # list tables in the database to verify
tables = pd.read_sql("SHOW TABLES", connection)
table_list = tables.iloc[:,0].tolist() # List of table names
print(f"Current tables in the database: {table_list}")
print(" ")

#-------------------------------------------
# Load processed data from MySQL
#-------------------------------------------
def load_table_from_mysql(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection)
    connection.close() # Close the connection after loading data
    print(f"Loaded table {table_name} from MySQL into DataFrame with shape {df.shape}")
    return df
# ----------------------------------------------------------
# Streamlit Dashboard and Visualizations
# ---------------------------------------------------------- 
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# Page settings
st.set_page_config(page_title="YouTube & TikTok Trends Dashboard", layout="wide")
st.title("📊 YouTube & TikTok Trends Dashboard")
st.markdown("Interactive dashboard visualizing trends from YouTube and TikTok data.")
st.markdown("---")
# Load Data
df = load_table_from_mysql(table_name)

# Sidebar filters
st.sidebar.header("Filters")
selected_platform = st.sidebar.multiselect(
    "Select Platform",
    options=df['platform'].unique(),
    default=df['platform'].unique()
)
selected_category = st.sidebar.multiselect(
    "Select Category",
    options=df['category'].unique(),
    default=df['category'].unique()
)

# Filter Data
filtered_df = df[
    (df['platform'].isin(selected_platform)) &
    (df['category'].isin(selected_category))
]

# Show filtered DataFrame
st.subheader("📄 Filtered Data")
st.dataframe(filtered_df)

# Apply corporate theme (minimal)
sns.set_style("whitegrid")
plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "legend.fontsize": 9
})

# ------------------------
# Plot 1: Views by Platform (Plotly)
# ------------------------
st.subheader("📈 Total Views by Platform")
views_by_platform = filtered_df.groupby('platform')['views'].sum().reset_index()

fig1 = px.bar(
    views_by_platform,
    x='platform', y='views', color='platform',
    title="Total Views by Platform",
    labels={"views":"Total Views", "platform":"Platform"},
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig1.update_layout(
    width=550, height=350,
    margin=dict(l=40, r=40, t=50, b=40),
    title_x=0.5,  # center title
    showlegend=False,
    font=dict(size=11)
)
st.plotly_chart(fig1, use_container_width=False)

# ------------------------
# Plot 2: Likes vs Dislikes (Matplotlib)
# ------------------------
st.subheader("👍 Likes vs 👎 Dislikes")
likes_dislikes = filtered_df.groupby('platform')[['likes', 'dislikes']].sum().reset_index()

fig2, ax = plt.subplots(figsize=(5.5,3.2))  # smaller fixed size
likes_dislikes.plot(
    kind='bar', x='platform', y=['likes', 'dislikes'], ax=ax,
    color=["#1f77b4", "#ff7f0e"], alpha=0.9
)
ax.set_title("Total Likes vs Dislikes by Platform", fontsize=12, pad=8)
ax.set_ylabel("Count", fontsize=10)
ax.set_xlabel("Platform", fontsize=10)
ax.legend(frameon=False, loc="upper right")
sns.despine()
st.pyplot(fig2)

# ------------------------
# Plot 3: Comments Distribution (Seaborn)
# ------------------------
st.subheader("💬 Comments Distribution by Platform")
fig3, ax = plt.subplots(figsize=(5.5,3.2))  # smaller fixed size
sns.boxplot(
    data=filtered_df, x='platform', y='comments',
    palette="Set2", ax=ax
)
ax.set_title("Comments Distribution by Platform", fontsize=12, pad=8)
ax.set_ylabel("Number of Comments", fontsize=10)
ax.set_xlabel("Platform", fontsize=10)
sns.despine()
st.pyplot(fig3)

# ------------------------
# Plot 4: Top 10 Videos by Views (Plotly)
# ------------------------
st.subheader("🏆 Top 10 Videos by Views")
top_videos = filtered_df.nlargest(10, 'views')[['title', 'views', 'platform']]
fig4 = px.bar(
    top_videos,
    x='views', y='title', color='platform',
    orientation='h',
    title="Top 10 Videos by Views",
    labels={"views":"Views", "title":"Video Title", "platform":"Platform"},
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig4.update_layout(
    width=550, height=350,
    margin=dict(l=40, r=40, t=50, b=40),
    title_x=0.5,
    showlegend=True,
    font=dict(size=11)
)
st.plotly_chart(fig4, use_container_width=False)

# ------------------------
# Plot 5: Engagement Rate by Platform (Matplotlib)
# ------------------------
st.subheader("📊 Engagement Rate by Platform")
engagement = filtered_df.groupby('platform').agg({
    'views': 'sum',
    'likes': 'sum',
    'dislikes': 'sum',
    'comments': 'sum'
}).reset_index()
engagement['engagement_rate'] = (engagement['likes'] + engagement['comments']) / engagement['views'] * 100
fig5, ax = plt.subplots(figsize=(5.5,3.2))  # smaller fixed size
engagement.plot(
    kind='bar', x='platform', y='engagement_rate', ax=ax,
    color="#2ca02c", alpha=0.9
)
ax.set_title("Engagement Rate by Platform (%)", fontsize=12, pad=8)
ax.set_ylabel("Engagement Rate (%)", fontsize=10)
ax.set_xlabel("Platform", fontsize=10)
ax.legend(frameon=False, loc="upper right")
sns.despine()
st.pyplot(fig5)

# ------------------------
# Plot 6: Category Distribution (Seaborn)
# ------------------------
st.subheader("📂 Category Distribution")
fig6, ax = plt.subplots(figsize=(5.5,3.2))  # smaller fixed size
sns.countplot(
    data=filtered_df, x='category',
    palette="Set2", ax=ax,
    order=filtered_df['category'].value_counts().index
)
ax.set_title("Number of Videos by Category", fontsize=12, pad=8)
ax.set_ylabel("Number of Videos", fontsize=10)
ax.set_xlabel("Category", fontsize=10)
plt.xticks(rotation=45, ha='right')
sns.despine()
st.pyplot(fig6)

# ------------------------
# Footer
# ------------------------
st.markdown("---")
st.markdown("Developed by Nagasantosh Chandrashekar | Data Source: YouTube & TikTok APIs")
st.markdown("© 2025 Digicrome. All rights reserved.")
st.markdown(" ")

# End of Script



