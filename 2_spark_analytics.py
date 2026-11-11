from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, round

spark = SparkSession.builder \
    .appName("IndianWeatherAnalytics") \
    .getOrCreate()

print("Running PySpark Analytics Job...")
df = spark.read.csv("indian_weather_master.csv", header=True, inferSchema=True)

df = df.withColumnRenamed("air_quality_PM2.5", "air_quality_pm25")

regional_summary = df.groupBy("region").agg(
    round(avg("temperature_celsius"), 2).alias("avg_temp"),
    max("temperature_celsius").alias("max_temp"),
    min("temperature_celsius").alias("min_temp"),
    round(avg("air_quality_pm25"), 2).alias("avg_pm25")
).orderBy(col("avg_temp").desc())

regional_summary.show(10)

regional_summary.toPandas().to_csv("regional_trends.csv", index=False)
print("Aggregated analytics exported to 'regional_trends.csv'")

spark.stop()