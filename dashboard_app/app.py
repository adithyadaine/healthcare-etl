import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Hospital Readmission Dashboard",
    page_icon="🏥",
    layout="wide",
)

# --- DATABASE CONNECTION ---
# Function to get the database connection
def get_engine():
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    return create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}')

# Cache the data loading to prevent re-running the query on every interaction
@st.cache_data
def load_data():
    engine = get_engine()
    query = "SELECT * FROM heart_failure_readmissions;"
    df = pd.read_sql(query, engine)
    return df

df = load_data()

# --- DASHBOARD UI ---
st.title("🏥 Hospital Readmission Dashboard for Heart Failure")
st.markdown("This dashboard analyzes data on hospital readmission rates for heart failure patients.")

# --- Key Metrics ---
st.header("Key Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Total Hospitals Analyzed", f"{df['facility_id'].nunique():,}")
col2.metric("Average Readmission Ratio", f"{df['excess_readmission_ratio'].mean():.3f}")
col3.metric("Total Discharges", f"{df['number_of_discharges'].sum():,}")

st.markdown("---")

# --- VISUALIZATIONS ---
st.header("Analysis and Visualizations")

# 1. Map of Average Readmission Ratio by State
st.subheader("Average Readmission Ratio by State")
state_agg = df.groupby('state')['excess_readmission_ratio'].mean().reset_index()
fig_map = px.choropleth(
    state_agg,
    locations='state',
    locationmode="USA-states",
    color='excess_readmission_ratio',
    scope="usa",
    color_continuous_scale="Viridis_r",
    title="Average Hospital Readmission Ratio for Heart Failure"
)
st.plotly_chart(fig_map, use_container_width=True)

# 2. Bar Chart of Readmission Ratio by Hospital Ownership
st.subheader("Readmission Ratio by Hospital Ownership")
ownership_agg = df.groupby('hospital_ownership')['excess_readmission_ratio'].mean().reset_index().sort_values(by='excess_readmission_ratio', ascending=False)
fig_bar = px.bar(
    ownership_agg,
    x='hospital_ownership',
    y='excess_readmission_ratio',
    title="Average Readmission Ratio by Hospital Ownership Type",
    labels={'excess_readmission_ratio': 'Average Excess Readmission Ratio', 'hospital_ownership': 'Hospital Ownership'},
    color='excess_readmission_ratio',
    color_continuous_scale="reds"
)
st.plotly_chart(fig_bar, use_container_width=True)

# 3. Data Table for Top/Bottom Hospitals
st.subheader("Explore Hospital Data")
sort_order = st.radio("Sort hospitals by:", ('Highest Readmission Ratio', 'Lowest Readmission Ratio'))
num_hospitals = st.slider("Number of hospitals to show:", 5, 50, 10)

if sort_order == 'Highest Readmission Ratio':
    sorted_df = df.sort_values(by='excess_readmission_ratio', ascending=False)
else:
    sorted_df = df.sort_values(by='excess_readmission_ratio', ascending=True)

st.dataframe(sorted_df.head(num_hospitals)[[
    'facility_name', 'city_town', 'state', 'excess_readmission_ratio', 'number_of_discharges'
]].reset_index(drop=True))