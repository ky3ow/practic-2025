# Databricks notebook source
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, date_format
)

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS sandbox.weather
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS sandbox.weather.dim_location (
    location_id INT,
    city STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timezone STRING
) USING delta
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS sandbox.weather.dim_date (
    date_id STRING,
    date DATE,
    year INT,
    month INT,
    day INT,
    weekday_name STRING
) USING delta
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS sandbox.weather.fact_weather (
    location_id INT,
    date_id STRING,
    hour INT,
    temperature_c DOUBLE,
    humidity DOUBLE,
    precipitation_mm DOUBLE,
    windspeed_ms DOUBLE,
    load_date DATE
) USING delta
""")

# COMMAND ----------

dim_location = spark.createDataFrame(
    [
        (1, "Lviv", 49.83826, 24.02324, "auto"),
        (2, "Ternopil", 49.55589, 25.60556, "auto"),
        (3, "Zhytomyr", 50.26487, 28.67669, "auto")
    ],
    ["location_id", "city", "latitude", "longitude", "timezone"]
)

dim_location.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("sandbox.weather.dim_location")

# COMMAND ----------

import requests
import pandas as pd
import datetime

# Read locations from UC
locations = spark.table("sandbox.weather.dim_location").collect()

# Prepopulate
prepopulate = False
if prepopulate:
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=30)
else:
    end_date = datetime.date.today()
    start_date = datetime.date.today()
all_data = []

for loc in locations:
    print(f"Fetching {loc['city']}")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": loc['latitude'],
        "longitude": loc['longitude'],
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,windspeed_10m",
        "timezone": loc['timezone'],
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.today().strftime("%Y-%m-%d")
    }
    resp = requests.get(url, params=params).json()
    hourly = pd.DataFrame(resp["hourly"])
    hourly["time"] = pd.to_datetime(hourly["time"])
    hourly["location_id"] = loc['location_id']
    hourly["load_date"] = datetime.date.today()
    all_data.append(hourly)

# Concatenate all cities data
pdf = pd.concat(all_data, ignore_index=True)
df = spark.createDataFrame(pdf)

# COMMAND ----------

display(df)

# COMMAND ----------

dim_date = df.select(
    date_format(col("time"), "yyyyMMdd").alias("date_id"),
    col("time").cast("date").alias("date"),
    year("time").alias("year"),
    month("time").alias("month"),
    dayofmonth("time").alias("day"),
    date_format("time", "EEEE").alias("weekday_name")
).distinct()

dim_date.createOrReplaceTempView("dim_date_update")

spark.sql("""
MERGE INTO sandbox.weather.dim_date t
USING dim_date_update s
ON t.date_id = s.date_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

display(dim_date)

# COMMAND ----------

fact_weather = df.withColumn("date_id", date_format(col("time"), "yyyyMMdd")) \
    .withColumn("hour", hour(col("time"))) \
    .select(
        col("location_id"),
        col("date_id"),
        col("hour"),
        col("temperature_2m").alias("temperature_c"),
        col("relativehumidity_2m").alias("humidity"),
        col("precipitation").alias("precipitation_mm"),
        col("windspeed_10m").alias("windspeed_ms"),
        col("load_date")
    )

fact_weather.createOrReplaceTempView("fact_weather_update")

spark.sql("""
MERGE INTO sandbox.weather.fact_weather t
USING fact_weather_update s
ON  t.location_id = s.location_id
AND t.date_id = s.date_id
AND t.hour = s.hour
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

display(fact_weather)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sandbox.weather.vw_weather_daily_summary AS
# MAGIC SELECT f.date_id,
# MAGIC        d.date,
# MAGIC        l.city,
# MAGIC        ROUND(AVG(f.temperature_c), 2) AS avg_temp,
# MAGIC        ROUND(MIN(f.temperature_c), 2) AS min_temp,
# MAGIC        ROUND(MAX(f.temperature_c), 2) AS max_temp,
# MAGIC        ROUND(SUM(f.precipitation_mm), 2) AS total_precip
# MAGIC FROM sandbox.weather.fact_weather f
# MAGIC JOIN sandbox.weather.dim_date d ON f.date_id = d.date_id
# MAGIC JOIN sandbox.weather.dim_location l ON f.location_id = l.location_id
# MAGIC GROUP BY f.date_id, d.date, l.city;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sandbox.weather.vw_weather_hourly_profile AS
# MAGIC SELECT l.city,
# MAGIC        f.date_id,
# MAGIC        f.hour,
# MAGIC        ROUND(AVG(f.temperature_c), 2) AS avg_temp,
# MAGIC        ROUND(AVG(f.precipitation_mm), 2) AS avg_rain
# MAGIC FROM sandbox.weather.fact_weather f
# MAGIC JOIN sandbox.weather.dim_location l ON f.location_id = l.location_id
# MAGIC GROUP BY l.city, f.date_id, f.hour
# MAGIC ORDER BY l.city, f.date_id, f.hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sandbox.weather.vw_weather_hourly_profile_last7days AS
# MAGIC SELECT l.city,
# MAGIC        f.hour,
# MAGIC        ROUND(AVG(f.temperature_c), 2) AS avg_temp,
# MAGIC        ROUND(AVG(f.precipitation_mm), 2) AS avg_rain
# MAGIC FROM sandbox.weather.fact_weather f
# MAGIC JOIN sandbox.weather.dim_location l ON f.location_id = l.location_id
# MAGIC JOIN sandbox.weather.dim_date d ON f.date_id = d.date_id
# MAGIC WHERE d.date >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY l.city, f.hour
# MAGIC ORDER BY l.city, f.hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sandbox.weather.vw_weather_hourly_profile_last30days AS
# MAGIC SELECT l.city,
# MAGIC        f.hour,
# MAGIC        ROUND(AVG(f.temperature_c), 2) AS avg_temp,
# MAGIC        ROUND(AVG(f.precipitation_mm), 2) AS avg_rain
# MAGIC FROM sandbox.weather.fact_weather f
# MAGIC JOIN sandbox.weather.dim_location l ON f.location_id = l.location_id
# MAGIC JOIN sandbox.weather.dim_date d ON f.date_id = d.date_id
# MAGIC WHERE d.date >= add_months(current_date(), -1)
# MAGIC GROUP BY l.city, f.hour
# MAGIC ORDER BY l.city, f.hour;
