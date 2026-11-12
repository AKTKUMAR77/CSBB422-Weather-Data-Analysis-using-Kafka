from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, round

spark = SparkSession.builder \
    .appName("CSBB422-Weather-Analytics-Hadoop-YARN") \
    .master("yarn") \
    .getOrCreate()

# 1. Reading data from HDFS
hdfs_input_path = "hdfs://master:9000/weather/input/indian_weather_master.csv"
df = spark.read.csv(hdfs_input_path, header=True, inferSchema=True)

for c in df.columns:
    df = df.withColumnRenamed(c, c.replace(".", "_").replace(" ", "_"))

print(f"Total Records Loaded from HDFS: {df.count()}")
df.printSchema()

# 2. City / Location-wise Weather & AQI Aggregation
group_col = "location_name" if "location_name" in df.columns else df.columns[0]

summary_df = df.groupBy(group_col).agg(
    count("*").alias("total_records"),
    round(avg("temperature_celsius"), 2).alias("avg_temp_c"),
    max("temperature_celsius").alias("max_temp_c"),
    min("temperature_celsius").alias("min_temp_c"),
    round(avg("humidity"), 2).alias("avg_humidity")
).orderBy(col("avg_temp_c").desc())

summary_df.show(15, truncate=False)

# 3. Distributed Output into HDFS
hdfs_output_path = "hdfs://master:9000/weather/output/weather_summary"
summary_df.coalesce(1).write.mode("overwrite").csv(hdfs_output_path, header=True)

# Local CSV
summary_df.toPandas().to_csv("yarn_weather_summary.csv", index=False)

print("Successfully processed on Hadoop YARN and saved results to HDFS!")
spark.stop()
