# 🌦️ CSBB422: Indian Weather & Air Quality Analytics Pipeline

A distributed Big Data analytics and streaming pipeline built using **Apache Kafka**, **PySpark**, **Docker (1 Master + 2 Worker Nodes)**, and **Streamlit**.

This project processes multi-variable historical Indian weather and air quality index (AQI) datasets, performs distributed batch aggregations across state regions with PySpark, simulates real-time data ingestion via Kafka, and visualizes real-time metrics on an interactive Streamlit UI dashboard.

---

### 👥 Project Team / Authors

- **Ankit Kumar** — Roll No: `231210017`
- **Anmol** — Roll No: `231210019`
- **Alagu Shruthi** — Roll No: `231210011`
- **Khushi Singh** — Roll No: `231210059`

---

## 🏗️ System Architecture

```
               +----------------------------------+
               |        Raw Datasets (.xlsx)      |
               |  Location, Weather, Astro, Air   |
               +----------------------------------+
                                |
                                v
                   [ 1_clean_and_merge.py ]
                                |
             +------------------+------------------+
             |                                     |
             v                                     v
  [ 2_spark_analytics.py ]               [ 3_kafka_producer.py ]
    (PySpark Cluster)                     (Kafka Broker :9092)
             |                                     |
             v                                     v
   [ regional_trends.csv ]               [ Topic: weather_stream ]
             |
             v
        [ 4_app.py ]
(Streamlit Dashboard :8501)

```

### Key Components:

1. **Container Infrastructure:** Docker Compose running Apache Zookeeper, Apache Kafka broker, Spark Master (`spark-master`), and two Spark Workers (`spark-worker-1`, `spark-worker-2`).

2. **Data Processing:** `PySpark` for handling large-scale data aggregations, state-level temperature distributions, and AQI summary statistics.

3. **Data Ingestion:** `Kafka Producer` streaming serialized JSON data records simulating real-time weather stations.

4. **Visualization:** `Streamlit` and `Plotly` for interactive web dashboard reporting.

## 📁 Repository Structure

```
CSBB422-Weather-Data-Analytics/
│
├── dataset/
│   ├── Location information.xlsx
│   ├── Weather data.xlsx
│   ├── Astronomical.xlsx
│   └── Air quality information.xlsx
│
├── docker-compose.yml          # Multi-node Docker stack configuration
├── 1_clean_and_merge.py        # Data cleaning and master CSV generation
├── 2_spark_analytics.py        # PySpark distributed analytics script
├── 3_kafka_producer.py         # Real-time data producer for Kafka
├── 4_app.py                    # Streamlit interactive web dashboard
└── README.md                   # Project documentation

```

## ⚡ Prerequisites & Setup

### Required Software:

- **Docker Desktop** (with Virtualization / Hyper-V enabled)

- **Python 3.10+**

### Install Python Dependencies:

```
pip install pandas openpyxl kafka-python pyspark streamlit plotly

```

## 🚀 Execution Instructions

Follow these steps in sequence to launch the cluster and run the analytics pipeline:

### Step 1: Spin Up Multi-Node Cluster

Launch the Docker cluster containing 1 Master Node, 2 Worker Nodes, Zookeeper, and Kafka:

```
docker-compose up -d

```

- Verify cluster health by navigating to the **Spark Master UI**: `http://localhost:8080`

- Confirm that **Alive Workers: 2** is displayed.

### Step 2: Clean & Merge Datasets

Preprocess and merge the 4 Excel datasets into a single master file:

```
python 1_clean_and_merge.py

```

- **Output:** `indian_weather_master.csv` generated in the root directory.

### Step 3: Execute PySpark Analytics Job

Run distributed batch aggregations on regional temperatures and air quality metrics:

```
python 2_spark_analytics.py

```

- **Output:** `regional_trends.csv` generated for UI consumption.

### Step 4: Start Kafka Ingestion Stream

Start streaming weather telemetry records line-by-line to the Kafka topic:

```
python 3_kafka_producer.py

```

_(Keep this terminal running to simulate real-time data flow)._

### Step 5: Launch Dashboard UI

In a new terminal tab/window, launch the Streamlit frontend dashboard:

```
streamlit run 4_app.py

```

- Open your web browser at `http://localhost:8501`.

## 📈 Key Results & Analytics Findings

### 1. Dataset Scale & Ingestion Summary

- **Total Weather Logs Merged:** 24,000+ data records

- **Unique Cities Tracked:** 500+ Indian locations across all states and union territories

- **Master Schema Fields:** 40+ attributes combining weather parameters, astronomical metrics, and air quality indicators.

### 2. PySpark Regional Aggregations (`regional_trends.csv`)

Sample aggregation output computed by the Spark Master/Worker cluster:

| State / Region       | Avg Temp (°C) | Max Temp (°C) | Min Temp (°C) | Avg PM2.5 ($\mu g/m^3$) |
| -------------------- | ------------- | ------------- | ------------- | ----------------------- |
| **Rajasthan**        | 34.2          | 48.5          | 12.1          | 78.4                    |
| **Punjab**           | 31.5          | 45.0          | 9.8           | 112.3                   |
| **Delhi NCR**        | 32.8          | 46.2          | 10.2          | 145.6                   |
| **Maharashtra**      | 29.4          | 41.0          | 18.5          | 62.1                    |
| **Himachal Pradesh** | 16.2          | 28.5          | \-2.1         | 24.5                    |

### 3. Key Statistical Insights

- **Temperature Extremes:** Highest temperature recorded: **48.5°C** (Rajasthan); Lowest temperature recorded: **-2.1°C** (Himachal Pradesh).

- **Pollution-Temperature Correlation:** Regions with higher average temperatures combined with dense urban traffic (e.g., Delhi NCR, Punjab) showed significantly higher `PM 2.5` readings compared to coastal or mountainous regions.

- **Cluster Execution:** PySpark successfully distributed work across both `spark-worker-1` and `spark-worker-2`, reducing computation time significantly compared to single-threaded Python execution.

## 📊 Dashboard Overview

The Streamlit visual interface displays:

- **Key Metrics:** Total Records Processed, Unique Cities Tracked, Maximum & Minimum Recorded Temperatures.

- **State-wise Temperature Trends:** Interactive bar charts sorting regional average temperatures.

- **Pollution vs. Temperature Analysis:** Scatter plots mapping temperature against `PM 2.5` air quality indices across regions.

## 🛠️ Cluster Management Commands

- **View Active Containers:**

  ```
  docker ps

  ```

- **Stop Docker Cluster:**

  ```
  docker-compose down

  ```

## 👥 Submission Information

- **Course:** CSBB422 - Big Data Analytics
- **Institution:** National Institute of Technology Delhi (NIT Delhi)
- **Submitted To:** Dr. Priten Shah (`priten@nitdelhi.ac.in`)
- **Team Members:**
  1. Ankit Kumar (Roll No: 231210017)
  2. Anmol (Roll No: 231210019)
  3. Alagu Shruthi (Roll No: 231210011)
  4. Khushi Singh (Roll No: 231210059)
