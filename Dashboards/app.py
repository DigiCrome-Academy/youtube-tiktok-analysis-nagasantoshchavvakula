"""
Script Name: app.py
Project: YouTube & TikTok Trends Dashboard

Purpose:
    This script builds an interactive Streamlit dashboard for visualizing YouTube and TikTok trends.
    It connects to a MySQL database containing processed social media data, loads CSV datasets
    into MySQL tables automatically, and generates interactive and static visualizations to 
    analyze engagement metrics such as views, likes, dislikes, and comments.

Workflow:
    1. Load MySQL credentials securely from Streamlit's Secrets Manager (`st.secrets`).
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
            d. Top 10 Videos by Views (Plotly horizontal bar)
            e. Engagement Rate by Platform (Matplotlib)
            f. Category Distribution (Seaborn countplot)
    6. Apply a consistent visualization theme using Seaborn and Matplotlib styling.
    7. Cache MySQL data for 10 minutes to improve dashboard performance.

Inputs:
    - MySQL database tables (created from `data/processed/` CSV files)
    - Streamlit Secrets for database credentials:
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
    - streamlit
    - matplotlib
    - seaborn
    - plotly
    - os

Usage:
    Run the following command from the terminal:
        streamlit run app.py

    This will launch the dashboard locally, accessible via a browser at:
        http://localhost:8501

Notes:
    - Ensure MySQL service is running and accessible before launching the dashboard.
    - Streamlit Secrets must contain valid database credentials.
    - All processed CSVs in `data/processed/` are automatically uploaded to MySQL tables.
    - Cached data improves performance but will refresh every 10 minutes.
"""

# ----------------------Importing Libraries---------------------- #
import os
import pandas as pd
from sqlalchemy import create_engine
import configparser
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# ---------------- Step 1: Print Current Working Directory ----------------
print("CWD:", os.getcwd())
print("Files:", os.listdir())

# ---------------- Step 2: Verify secrets file path (local only) ----------------
# This is only for local debugging — Streamlit Cloud uses its built-in Secrets Manager
secrets_path = os.path.join(os.getcwd(), "Dashboards", ".streamlit", "secrets.toml")
print("Looking for secrets file at:", secrets_path)
print("Secrets file exists:", os.path.exists(secrets_path))

# ---------------- Step 3: Load MySQL credentials from Streamlit secrets ----------------
try:
    db_config = st.secrets["mysql"]

    DB_HOST = db_config["host"]
    DB_USER = db_config["user"]
    DB_PASSWORD = db_config["password"]
    DB_NAME = db_config["database"]

    # ---------------- Step 4: Print confirmation ----------------
    print("✅ MySQL Configuration loaded successfully:")
    print(f"Host: {DB_HOST}")
    print(f"User: {DB_USER}")
    print(f"Database: {DB_NAME}")

except Exception as e:
    st.error(f"❌ Failed to load database configuration: {e}")
    print("❌ Error loading Streamlit secrets:", e)

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

    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False) # Load DataFrame into MySQL table
        print(f"Loaded data into table '{table_name}' in the database.")
    except Exception as e:
        print(f"Error loading data into table '{table_name}': {e}")

    print(" ")
    # list tables in the database to verify
tables = pd.read_sql("SHOW TABLES", connection)
table_list = tables.iloc[:,0].tolist() # List of table names
print(f"Current tables in the database: {table_list}")
print(" ")

#-------------------------------------------
# Load processed data from MySQL
#-------------------------------------------
@st.cache_data(ttl=600)  # Cache the data for 10 minutes to improve performance
def load_table_from_mysql(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection)
    print(f"Loaded table {table_name} from MySQL into DataFrame with shape {df.shape}")
    return df

# ----------------------------------------------------------
# Streamlit Dashboard and Visualizations
# ---------------------------------------------------------- 

# Page settings
st.set_page_config(page_title="YouTube & TikTok Trends Dashboard", layout="wide")
st.title("📊 YouTube & TikTok Trends Dashboard")
st.markdown("Interactive dashboard visualizing trends from YouTube and TikTok data.")
st.markdown("---")
# Load Data
df = load_table_from_mysql(table_name)

# 🌈 Stylish Sidebar Filters
st.sidebar.markdown("## 🎛️ Data Filters")

# Divider line
st.sidebar.markdown("---")

# Platform Filter
st.sidebar.markdown("### 💻 Platform")
selected_platform = st.sidebar.multiselect(
    "Choose one or more platforms:",
    options=sorted(df['platform'].unique()),
    default=df['platform'].unique(),
    help="Filter data by platform (e.g., YouTube, Instagram, etc.)"
)

# Category Filter
st.sidebar.markdown("### 🏷️ Category")
selected_category = st.sidebar.multiselect(
    "Choose one or more categories:",
    options=sorted(df['category'].unique()),
    default=df['category'].unique(),
    help="Filter data by category type"
)

# Divider line
st.sidebar.markdown("---")
st.sidebar.info("✅ Use the filters above to customize your dashboard view.")

# Filter the DataFrame
filtered_df = df[
    (df['platform'].isin(selected_platform)) &
    (df['category'].isin(selected_category))
]

# Show filtered DataFrame
st.subheader("📄 Filtered Data")
st.dataframe(filtered_df)

# Apply theme (minimal)
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

# Group data by platform and sum likes/dislikes
likes_dislikes = filtered_df.groupby('platform')[['likes', 'dislikes']].sum().reset_index()

# Check if there's data to plot
if likes_dislikes.empty:
    st.warning("⚠️ No data available for the selected filters.")
else:
    fig2, ax = plt.subplots(figsize=(5.5, 3.2))  # smaller fixed size
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

# ----------------------End of Dashboard---------------------- #

# Closing DB connection
connection.close()
print("Database connection closed.")
print(" ")


