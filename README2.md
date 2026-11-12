# 3-Node Multi-Node Hadoop, Spark, YARN & Kafka Weather Analytics System

This document serves as the comprehensive setup, configuration, operation, and viva reference guide for the multi-node distributed Big Data infrastructure.

---

## 1. System & Cluster Architecture

The system consists of a 3-node virtualized cluster running on VirtualBox with Ubuntu Linux, configured with a Host-Only Network Adapter (`192.168.56.0/24`) for inter-node communication and host access.

| Node Name     | Role                                                 | IP Address       | Configured Specs  | Daemons Running                                                          |
| :------------ | :--------------------------------------------------- | :--------------- | :---------------- | :----------------------------------------------------------------------- |
| **`master`**  | NameNode, ResourceManager, Kafka Broker, Driver Node | `192.168.56.101` | 8 GB RAM, 4 vCPUs | `NameNode`, `SecondaryNameNode`, `ResourceManager`, `Kafka`, `Streamlit` |
| **`worker1`** | DataNode, NodeManager, Spark Executor                | `192.168.56.102` | 4 GB RAM, 2 vCPUs | `DataNode`, `NodeManager`                                                |
| **`worker2`** | DataNode, NodeManager, Spark Executor                | `192.168.56.103` | 4 GB RAM, 2 vCPUs | `DataNode`, `NodeManager`                                                |

### Core Software Stack

- **OS:** Ubuntu 24.04 LTS
- **Java Development Kit:** OpenJDK 11 (`/usr/lib/jvm/java-11-openjdk-amd64`)
- **Hadoop Framework:** Apache Hadoop 3.3.6
- **Distributed Engine:** Apache Spark 3.5.3 (configured for YARN execution)
- **Streaming Platform:** Apache Kafka 3.7.1 (KRaft Mode on `master:9092`)
- **Analytics & UI:** Python 3.12 (`pyspark`, `pandas`, `kafka-python-ng`, `streamlit`)

---

## 2. Distributed Data & Execution Flow

1. **Ingestion & Preprocessing (`1_clean_and_merge.py`):**
   - Merges raw regional weather and air quality datasets into `indian_weather_master.csv`.
   - Loads merged dataset into HDFS path: `hdfs://master:9000/weather/input/` with a replication factor of $2$ across `worker1` and `worker2`.
2. **Distributed Analytics on YARN (`2_spark_analytics.py` / `2_spark_yarn.py`):**
   - Submits PySpark applications using `spark-submit --master yarn --deploy-mode client`.
   - YARN distributes tasks across Executors on `worker1` and `worker2`.
   - Aggregates temperature, humidity, and $\text{PM}_{2.5}$ metrics.
   - Writes distributed results back to HDFS (`/weather/output/`) and exports local CSV (`regional_trends.csv`) for UI rendering.
3. **Real-Time Event Streaming (`3_kafka_producer.py`):**
   - Streams live weather telemetry events to Apache Kafka KRaft Broker running on `localhost:9092`.
4. **Interactive Dashboard (`4_app.py`):**
   - Streamlit dashboard served on `0.0.0.0:8501`, accessible via Host-Only network from Windows host at `http://192.168.56.101:8501`.

---

## 3. Web UI Interfaces Reference

When the cluster is active, access these management dashboards from the Windows Host browser:

| Service                  | Web UI URL                   | Purpose                     | Key Verification Metric                    |
| :----------------------- | :--------------------------- | :-------------------------- | :----------------------------------------- |
| **HDFS NameNode**        | `http://192.168.56.101:9870` | HDFS Health & File Browser  | **Live Nodes: 2**, Check `/weather/input`  |
| **YARN ResourceManager** | `http://192.168.56.101:8088` | Cluster Resource Management | **Active Nodes: 2**, App State `SUCCEEDED` |
| **Streamlit Dashboard**  | `http://192.168.56.101:8501` | User Analytics Interface    | Interactive Weather/AQI Visualizations     |

---

## 4. One-Time Cluster Setup & Installation (Historical Reference)

### Step 4.1: SSH Connection to Master Node

```powershell
# From Host Windows PowerShell
ssh vboxuser@192.168.56.101
```

### Step 4.2: Install Software Dependencies (`master`)

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk openssh-server pdsh python3-pip wget
pip3 install --break-system-packages pandas openpyxl pyspark streamlit plotly kafka-python-ng

# Install Hadoop & Spark
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz

tar -xzf hadoop-3.3.6.tar.gz
tar -xzf spark-3.5.3-bin-hadoop3.tgz

sudo mv hadoop-3.3.6 /usr/local/hadoop
sudo mv spark-3.5.3-bin-hadoop3 /usr/local/spark
sudo chown -R $USER:$USER /usr/local/hadoop /usr/local/spark
rm -f hadoop-3.3.6.tar.gz spark-3.5.3-bin-hadoop3.tgz
```

### Step 4.3: Install Apache Kafka (`master`)

```bash
wget https://archive.apache.org/dist/kafka/3.7.1/kafka_2.13-3.7.1.tgz
tar -xzf kafka_2.13-3.7.1.tgz
sudo mv kafka_2.13-3.7.1 /usr/local/kafka
sudo chown -R $USER:$USER /usr/local/kafka
rm -f kafka_2.13-3.7.1.tgz

# Format KRaft Storage
KAFKA_CLUSTER_ID="$(/usr/local/kafka/bin/kafka-storage.sh random-uuid)"
/usr/local/kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /usr/local/kafka/config/kraft/server.properties
```

### Step 4.4: Configure Environment Variables (`~/.bashrc`)

```bash
cat << 'EOF' >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PDSH_RCMD_TYPE=ssh
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
EOF

source ~/.bashrc
```

### Step 4.5: Network Hosts Setup (All 3 Nodes)

Ensure `/etc/hosts` contains only valid cluster IP mappings and that the default Ubuntu loopback alias (`127.0.1.1`) is removed:

```bash
sudo sed -i '/127.0.1.1/d' /etc/hosts
sudo bash -c 'cat << EOF >> /etc/hosts
192.168.56.101 master
192.168.56.102 worker1
192.168.56.103 worker2
EOF'
```

### Step 4.6: Passwordless SSH Setup (From `master` to All Nodes)

```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
ssh-copy-id vboxuser@master
ssh-copy-id vboxuser@worker1
ssh-copy-id vboxuser@worker2
```

---

## 5. Daily Operation Runbook (Demonstration & Execution Steps)

Follow this section to start the cluster after booting the VirtualBox VMs (`master`, `worker1`, `worker2`). **Do NOT run `hdfs namenode -format` again**, as it wipes existing data.

### Step 5.1: Start Services (`master`)

Open SSH session to `master` and launch daemons:

```bash
# 1. Start HDFS Distributed Storage
start-dfs.sh

# 2. Start YARN Resource Manager & Node Managers
start-yarn.sh

# 3. Start Apache Kafka Server in Background
/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/kraft/server.properties

# 4. Verify Active Daemons
jps
```

#### Expected Daemon Outputs (`jps` Check):

- **On `master`:** `NameNode`, `SecondaryNameNode`, `ResourceManager`, `Kafka`, `Jps`
- **On `worker1` & `worker2`:** `DataNode`, `NodeManager`, `Jps`

---

### Step 5.2: Ingest Data & Run Distributed Spark Job

```bash
cd ~/BDA

# Set Python Environment for Spark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Run Data Preprocessing & Upload Master File to HDFS
python3 1_clean_and_merge.py
hdfs dfs -mkdir -p /weather/input /weather/output
hdfs dfs -put -f indian_weather_master.csv /weather/input/

# Submit PySpark Analytics Job to YARN Cluster
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1G \
  2_spark_analytics.py
```

---

### Step 5.3: Verify Output in HDFS

Check that PySpark wrote aggregated results into HDFS:

```bash
hdfs dfs -ls /weather/output/regional_trends/
hdfs dfs -cat /weather/output/regional_trends/part-*.csv | head -n 15
```

---

### Step 5.4: Launch Streamlit Dashboard & Real-Time Kafka Stream

Open **two separate SSH terminals** for `master`:

- **Terminal 1 (Streamlit Dashboard):**

  ```bash
  cd ~/BDA
  streamlit run 4_app.py --server.address 0.0.0.0
  ```

  - Access in Windows browser at: **`http://192.168.56.101:8501`**

- **Terminal 2 (Kafka Telemetry Producer):**

  ```bash
  cd ~/BDA
  python3 3_kafka_producer.py
  ```

- **Terminal 3 (Optional - Verify Live Stream Messages via Kafka Consumer):**
  ```bash
  /usr/local/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic weather_stream \
    --from-beginning
  ```

---

## 6. Clean Shutdown Sequence

To prevent corruption of HDFS block metadata and Kafka transactional logs, stop services in reverse order prior to shutting down VMs:

```bash
# 1. Stop Kafka Server
/usr/local/kafka/bin/kafka-server-stop.sh

# 2. Stop YARN Framework
stop-yarn.sh

# 3. Stop HDFS Filesystem
stop-dfs.sh

# 4. Gracefully Power Off VMs
sudo shutdown -h now
```

_(Repeat `sudo shutdown -h now` on `worker1` and `worker2` terminals)._

---

## 7. Troubleshooting & Frequently Encountered Issues

### Issue 1: `0 Live Nodes` on HDFS UI (`9870`) or YARN UI (`8088`) Unreachable

- **Cause:** Default Ubuntu IP alias `127.0.1.1 master` bound daemons to loopback interface.
- **Fix:** Execute `sudo sed -i '/127.0.1.1/d' /etc/hosts` across all 3 nodes, then restart HDFS and YARN (`stop-dfs.sh`, `stop-yarn.sh`, `start-dfs.sh`, `start-yarn.sh`).

### Issue 2: `SyntaxError: invalid decimal literal` when running `.py` scripts

- **Cause:** Non-breaking spaces (`U+00A0`) pasted from Windows clipboard.
- **Fix:** Execute `sed -i 's/\xc2\xa0/ /g' script_name.py` to convert all non-standard spaces back to standard ASCII spaces.

### Issue 3: `ModuleNotFoundError: No module named 'kafka'`

- **Cause:** Missing Python Kafka package compatible with Python 3.12.
- **Fix:** Execute `pip3 install --break-system-packages kafka-python-ng`.

---

## 8. Summary of Project Script Files (`~/BDA/`)

- **`1_clean_and_merge.py`**: Merges regional datasets into a unified master dataset (`indian_weather_master.csv`).
- **`2_spark_analytics.py`**: Reads master dataset from HDFS (`hdfs://master:9000/weather/input/indian_weather_master.csv`), computes regional aggregations via PySpark on YARN, writes output to HDFS and exports `regional_trends.csv`.
- **`2_spark_yarn.py`**: Extended city/location-wise aggregation script operating natively over YARN.
- **`3_kafka_producer.py`**: Streams real-time simulated weather events to Kafka topic (`localhost:9092`).
- **`4_app.py`**: Streamlit interactive visualization interface displaying real-time metrics and historical analytics.
