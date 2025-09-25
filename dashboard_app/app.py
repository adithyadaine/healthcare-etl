import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# --- PAGE CONFIGURATION ---
# Sets the title, icon, and layout for the web page.
st.set_page_config(
    page_title="Hospital Readmission Dashboard",
    page_icon="🏥",
    layout="wide",
)

# --- DATABASE CONNECTION ---
# This function creates a database connection engine. It reads credentials from environment variables.
def get_engine():
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    return create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}')

# The `@st.cache_data` decorator is a powerful Streamlit feature.
# It tells Streamlit to run this function once and then store the result in cache.
# The data will only be reloaded from the database if the code of this function changes.
# This prevents slow database queries every time a user interacts with a widget.
@st.cache_data
def load_data():
    engine = get_engine()
    query = "SELECT * FROM heart_failure_readmissions;"
    df = pd.read_sql(query, engine)
    return df

# Load the data into a DataFrame.
df = load_data()

# --- DASHBOARD UI ---
# All `st.` commands create elements on the web page.
st.title("🏥 Hospital Readmission Dashboard for Heart Failure")
st.markdown("This dashboard analyzes data on hospital readmission rates for heart failure patients.")

# --- Key Metrics ---
st.header("Key Metrics")
col1, col2, col3 = st.columns(3) # Creates three columns for a clean layout.
# `st.metric` displays a single number in a prominent way.
col1.metric("Total Hospitals Analyzed", f"{df['facility_id'].nunique():,}")
col2.metric("Average Readmission Ratio", f"{df['excess_readmission_ratio'].mean():.3f}")
# (Column 3 is left empty for layout purposes)

# --- VISUALIZATIONS ---
st.header("Visualizations")

# 1. Choropleth Map of Readmission Ratios by State
st.subheader("Geographic Distribution of Readmission Ratios")
# Group data by state and calculate the average readmission ratio.
state_agg = df.groupby('state')['excess_readmission_ratio'].mean().reset_index()
# Use Plotly Express to create the map.
fig_map = px.choropleth(
    state_agg,
    locations='state', # Column with state abbreviations.
    locationmode="USA-states", # Tells Plotly these are US states.
    color='excess_readmission_ratio', # The value to represent with color.
    scope="usa", # Focus the map on the USA.
    color_continuous_scale="Viridis_r", # The color scheme (reversed).
    title="Average Hospital Readmission Ratio for Heart Failure"
)
# Display the Plotly figure in the Streamlit app.
st.plotly_chart(fig_map, use_container_width=True)

# 2. Bar Chart of Readmission Ratio by Hospital Ownership
st.subheader("Readmission Ratio by Hospital Ownership")
# Group data by ownership type and calculate the average.
ownership_agg = df.groupby('hospital_ownership')['excess_readmission_ratio'].mean().reset_index().sort_values(by='excess_readmission_ratio', ascending=False)
# Create a bar chart with Plotly Express.
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
# `st.radio` creates radio buttons for user selection.
sort_order = st.radio("Sort hospitals by:", ('Highest Readmission Ratio', 'Lowest Readmission Ratio'))
# `st.slider` creates a slider for selecting a number.
num_hospitals = st.slider("Number of hospitals to show:", 5, 50, 10)

# Sort the DataFrame based on the user's radio button selection.
if sort_order == 'Highest Readmission Ratio':
    sorted_df = df.sort_values(by='excess_readmission_ratio', ascending=False)
else:
    sorted_df = df.sort_values(by='excess_readmission_ratio', ascending=True)

# Display the top N rows of the sorted DataFrame.
st.dataframe(sorted_df.head(num_hospitals))