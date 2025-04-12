from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType
import pandas as pd
import geopandas as gpd
import os

os.environ['HADOOP_HOME'] = "C:/Users/daneb/hadoop-3.3.5"
os.environ['PATH'] += os.pathsep + "C:/Users/daneb/hadoop-3.3.5/bin"
os.environ["PYSPARK_PYTHON"] = "C:/Users/daneb/AppData/Local/Programs/Python/Python39/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]

spark = SparkSession.builder \
    .appName("NYC TAXI") \
    .config("spark.jars.packages", ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.5.4"
])) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


gdf = gpd.read_file('C:/Users/daneb/Documents/taxi-data-pipeline/data/NYC Taxi Zones.geojson')
gdf = gdf[['location_id', 'zone', 'borough']]
gdf['location_id'] = gdf['location_id'].astype(int)

zones_pd = pd.DataFrame(gdf)
zones_spark = spark.createDataFrame(zones_pd)
zones_spark.cache()


df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nyc_taxi_rides") \
    .load()

schema = StructType() \
    .add("tpep_pickup_datetime", StringType()) \
    .add("tpep_dropoff_datetime", StringType()) \
    .add("trip_distance", StringType()) \
    .add("fare_amount", StringType()) \
    .add("tip_amount", StringType()) \
    .add("passenger_count", StringType()) \
    .add("PULocationID", StringType()) \
    .add("DOLocationID", StringType())

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

df_typed = df_parsed \
    .withColumn("pickup_time", to_timestamp("tpep_pickup_datetime")) \
    .withColumn("dropoff_time", to_timestamp("tpep_dropoff_datetime")) \
    .withColumn("trip_distance", col("trip_distance").cast(FloatType())) \
    .withColumn("fare_amount", col("fare_amount").cast(FloatType())) \
    .withColumn("tip_amount", col("tip_amount").cast(FloatType())) \
    .withColumn("trip_duration_minutes",
                (col("dropoff_time").cast("long") - col("pickup_time").cast("long")) / 60)


df_flagged = df_typed.withColumn(
    "valid",
    (col("trip_distance") > 0) & (col("trip_distance") < 100) &
    (col("fare_amount") > 0) &
    (col("tip_amount") >= 0) &
    (col("pickup_time").isNotNull()) &
    (col("dropoff_time").isNotNull()) &
    (col("passenger_count").isNotNull())
)

df_clean = df_flagged.filter(col("valid") == True)
df_invalid = df_flagged.filter(col("valid") == False)


df_enriched = df_clean \
    .join(zones_spark.withColumnRenamed("location_id", "PU_location_id"),
          df_clean["PULocationID"].cast("int") == col("PU_location_id"),
          how="left") \
    .withColumnRenamed("zone", "PU_Zone") \
    .withColumnRenamed("borough", "PU_Borough") \
    .drop("PU_location_id")

df_enriched = df_enriched \
    .join(zones_spark.withColumnRenamed("location_id", "DO_location_id"),
          df_clean["DOLocationID"].cast("int") == col("DO_location_id"),
          how="left") \
    .withColumnRenamed("zone", "DO_Zone") \
    .withColumnRenamed("borough", "DO_Borough") \
    .drop("DO_location_id")




def write_valid_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/taxi_data_pipeline") \
        .option("dbtable", "trips") \
        .option("user", "postgres") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def write_invalid_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/taxi_data_pipeline") \
        .option("dbtable", "invalid_trips") \
        .option("user", "postgres") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query_valid = df_enriched.writeStream \
    .foreachBatch(write_valid_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "file:///C:/checkpoints/postgres_valid_sink") \
    .start()

query_invalid = df_invalid.writeStream \
    .foreachBatch(write_invalid_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "file:///C:/checkpoints/postgres_invalid_sink") \
    .start()

query_console = df_enriched.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .start()

query_valid.awaitTermination()
query_invalid.awaitTermination()
query_console.awaitTermination()