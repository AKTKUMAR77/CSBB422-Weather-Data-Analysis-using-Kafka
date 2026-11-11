import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Indian Weather & AQI Dashboard", layout="wide")

st.title("🌦️ Indian Weather & Air Quality Analytics")
st.markdown("Distributed Pipeline: **Hadoop/Docker, Apache Kafka, PySpark, Streamlit**")

df_master = pd.read_csv("indian_weather_master.csv")
df_trends = pd.read_csv("regional_trends.csv")

# Metric cards
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Records", f"{len(df_master):,}")
col2.metric("Cities Tracked", f"{df_master['location_name'].nunique()}")
col3.metric("Max Temp", f"{df_master['temperature_celsius'].max()} °C")
col4.metric("Min Temp", f"{df_master['temperature_celsius'].min()} °C")

st.divider()

# Plot 1: Regional Avg Temp Bar Chart
st.subheader("📊 State-wise Average Temperature Trends")
fig1 = px.bar(df_trends, x="region", y="avg_temp", color="avg_temp", color_continuous_scale="Reds")
st.plotly_chart(fig1, use_container_width=True)

# Plot 2: Pollution vs Temp Scatter Plot
st.subheader("🌡️ Temperature vs PM 2.5 Pollution Index")
fig2 = px.scatter(df_master, x="temperature_celsius", y="air_quality_PM2.5", color="region", hover_data=["location_name"])
st.plotly_chart(fig2, use_container_width=True)