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
#-------------------------------------------#
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


