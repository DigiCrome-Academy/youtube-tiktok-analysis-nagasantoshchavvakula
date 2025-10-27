"""
Script: Dashboards/app.py
Project: YouTube & TikTok Trends Dashboard

Description:
This Streamlit application provides an interactive dashboard to explore and analyze engagement metrics from YouTube and TikTok datasets. 
Users can select from multiple processed CSV datasets, filter data by platform and category, and toggle various visualizations to gain insights into video performance.

Purpose:
To offer a user-friendly interface for visualizing key metrics such as views, likes, comments, and engagement rates across different platforms and categories.

Key Features:
- Load multiple processed CSV datasets from `data/processed/`
- Interactive sidebar for dataset selection and filtering by platform and category
- Toggle various visualizations including views, likes, comments, and engagement rates
- Display top N videos based on views
- Professional styling with Streamlit, Matplotlib, Seaborn, and Plotly

Dependencies:
- streamlit
- pandas
- matplotlib
- seaborn
- plotly

run with: `streamlit run Dashboards/app.py`
"""

# ---------------------- Import Libraries ---------------------- #
import os
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

# ---------------------- Page Configuration ---------------------- #
st.set_page_config(
    page_title="📊 YouTube & TikTok Trends Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------- Dashboard Header ---------------------- #
st.markdown("""
<style>
h1 {
    color: #2F4F4F;
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}
</style>
""", unsafe_allow_html=True)

st.title("📊 YouTube & TikTok Trends Dashboard")
st.markdown(
    "Explore and analyze engagement metrics from YouTube and TikTok datasets "
    "with interactive filters and professional visualizations."
)
st.markdown("---")

# ---------------------- Load CSV Datasets ---------------------- #
data_dir = 'data/processed/'
csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

if not csv_files:
    st.error("No CSV files found in `data/processed/`. Please add processed datasets.")
    st.stop()

@st.cache_data(ttl=600)
def load_csv_files(files):
    dataframes = {}
    for file in files:
        path = os.path.join(data_dir, file)
        try:
            df = pd.read_csv(path)
            dataframes[file] = df
        except Exception as e:
            st.warning(f"Could not load {file}: {e}")
    return dataframes

dataframes = load_csv_files(csv_files)

# ------------------------------------- Sidebar ---------------------------------------------------------- #
st.sidebar.title("Dashboard Controls")
st.sidebar.markdown("## 📂 Dataset & Filters")

# Dataset selection
dataset_name = st.sidebar.selectbox("Select Dataset", list(dataframes.keys()))
df = dataframes[dataset_name]
# ---------------------- Display Selected Dataset ---------------------- #
st.markdown(f"### 📂 Selected Dataset: `{dataset_name}`")
st.markdown(f"**Total Rows:** {df.shape[0]} | **Total Columns:** {df.shape[1]}")
st.dataframe(df.head(10))  # Show first 10 rows for preview
st.markdown("---")

# Platform filter
platform_options = df['platform'].unique() if 'platform' in df.columns else []
selected_platform = st.sidebar.multiselect("Filter by Platform", options=platform_options, default=platform_options)

# Category filter
category_options = df['category'].unique() if 'category' in df.columns else []
selected_category = st.sidebar.multiselect("Filter by Category", options=category_options, default=category_options)

# Number of top videos
top_n = st.sidebar.slider("Number of Top Videos to Display", min_value=5, max_value=20, value=10, step=1)

# ---------------------- Toggle Visualizations ---------------------- #
st.sidebar.markdown("---")
st.sidebar.markdown("### Toggle Charts")

# Master "Select All" checkbox
select_all = st.sidebar.checkbox("Select/Deselect All Charts", value=True)

# Individual chart checkboxes
show_views_platform = st.sidebar.checkbox("Views by Platform", value=select_all)
show_views_category = st.sidebar.checkbox("Views by Category", value=select_all)
show_likes_dislikes = st.sidebar.checkbox("Likes vs Dislikes", value=select_all)
show_comments = st.sidebar.checkbox("Comments Distribution", value=select_all)
show_engagement = st.sidebar.checkbox("Engagement Rate", value=select_all)
show_top_videos = st.sidebar.checkbox("Top Videos", value=select_all)
show_category_dist = st.sidebar.checkbox("Category Distribution", value=select_all)


# ---------------------- Apply Filters ---------------------- #
filtered_df = df.copy()
if selected_platform:
    filtered_df = filtered_df[filtered_df['platform'].isin(selected_platform)]
if selected_category:
    filtered_df = filtered_df[filtered_df['category'].isin(selected_category)]

# ---------------------- Dashboard Metrics ---------------------- #
st.markdown("### 📈 Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Videos", filtered_df.shape[0])
col2.metric("Total Views", int(filtered_df['views'].sum()) if 'views' in filtered_df.columns else 0)
col3.metric("Total Likes", int(filtered_df['likes'].sum()) if 'likes' in filtered_df.columns else 0)
col4.metric("Total Comments", int(filtered_df['comments'].sum()) if 'comments' in filtered_df.columns else 0)

st.markdown("---")

# ---------------------- Visualizations ---------------------- #
sns.set_style("whitegrid")
plt.rcParams.update({"font.size": 10, "axes.titlesize": 12, "axes.labelsize": 10, "legend.fontsize": 9})
CORP_COLORS = px.colors.qualitative.Pastel

# Views by Category
if show_views_category and 'category' in filtered_df.columns and 'views' in filtered_df.columns:
    st.subheader("📊 Total Views by Category")
    views_category = filtered_df.groupby('category')['views'].sum().reset_index()
    fig = px.bar(views_category, x='category', y='views', color='category', title="Total Views by Category", color_discrete_sequence=CORP_COLORS)
    st.plotly_chart(fig, use_container_width=True)

# Views by Platform
if show_views_platform and 'platform' in filtered_df.columns and 'views' in filtered_df.columns:
    st.subheader("📈 Total Views by Platform")
    views_platform = filtered_df.groupby('platform')['views'].sum().reset_index()
    fig = px.bar(views_platform, x='platform', y='views', color='platform', title="Total Views by Platform", color_discrete_sequence=CORP_COLORS)
    st.plotly_chart(fig, use_container_width=True)

# Likes vs Dislikes
if show_likes_dislikes and all(col in filtered_df.columns for col in ['platform','likes','dislikes']):
    st.subheader("👍 Likes vs 👎 Dislikes")
    likes_dis_df = filtered_df.groupby('platform')[['likes','dislikes']].sum().reset_index()
    fig, ax = plt.subplots(figsize=(7,4))
    likes_dis_df.plot(kind='bar', x='platform', y=['likes','dislikes'], ax=ax, color=["#4B8BBE","#FFB347"], alpha=0.9)
    ax.set_title("Likes vs Dislikes by Platform")
    ax.set_ylabel("Count"); ax.set_xlabel("Platform")
    sns.despine()
    st.pyplot(fig)

# Comments Distribution
if show_comments and all(col in filtered_df.columns for col in ['platform','comments']):
    st.subheader("💬 Comments Distribution by Platform")
    fig, ax = plt.subplots(figsize=(7,4))
    sns.boxplot(data=filtered_df, x='platform', y='comments', palette="Pastel2", ax=ax)
    ax.set_title("Comments Distribution")
    ax.set_ylabel("Number of Comments"); ax.set_xlabel("Platform")
    sns.despine()
    st.pyplot(fig)

# Engagement Rate
if show_engagement and all(col in filtered_df.columns for col in ['platform','views','likes','comments']):
    st.subheader("📊 Engagement Rate by Platform")
    eng_df = filtered_df.groupby('platform').agg({'views':'sum','likes':'sum','comments':'sum'}).reset_index()
    eng_df['engagement_rate'] = (eng_df['likes'] + eng_df['comments']) / eng_df['views'] * 100
    fig, ax = plt.subplots(figsize=(7,4))
    eng_df.plot(kind='bar', x='platform', y='engagement_rate', ax=ax, color="#2ca02c", alpha=0.9)
    ax.set_ylabel("Engagement Rate (%)"); ax.set_xlabel("Platform")
    ax.set_title("Engagement Rate by Platform")
    sns.despine()
    st.pyplot(fig)

# Top N Videos
if show_top_videos and 'views' in filtered_df.columns and 'title' in filtered_df.columns:
    st.subheader(f"🏆 Top {top_n} Videos by Views")
    top_videos = filtered_df.nlargest(top_n, 'views')[['title','views','platform']]
    fig = px.bar(top_videos, x='views', y='title', color='platform', orientation='h', title=f"Top {top_n} Videos by Views", color_discrete_sequence=CORP_COLORS)
    st.plotly_chart(fig, use_container_width=True)

# Category Distribution
if show_category_dist and 'category' in filtered_df.columns:
    st.subheader("📂 Category Distribution")
    fig, ax = plt.subplots(figsize=(8,4))
    sns.countplot(data=filtered_df, x='category', palette="Set2", order=filtered_df['category'].value_counts().index, ax=ax)
    ax.set_title("Number of Videos by Category")
    plt.xticks(rotation=45, ha='right')
    sns.despine()
    st.pyplot(fig)

# ---------------------- Footer ---------------------- #
st.markdown("---")
st.markdown("Developed by Nagasantosh Chandrashekar Chavvakula")
st.markdown("© 2025 Digicrome. All rights reserved.")
st.markdown(" ")