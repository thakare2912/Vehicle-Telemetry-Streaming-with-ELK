from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# 1. Spark Session with Elasticsearch config
spark = SparkSession.builder \
    .appName("VehicleTelemetryToElasticsearch") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .config("es.nodes", "elasticsearch") \
    .config("es.port", "9200") \
    .config("es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema
tire_schema = StructType([
    StructField("front_left", DoubleType()),
    StructField("front_right", DoubleType()),
    StructField("rear_left", DoubleType()),
    StructField("rear_right", DoubleType())
])

schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("user_name", StringType()),
    StructField("vehicle_number", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("manufacturer", StringType()),
    StructField("model", StringType()),
    StructField("year", IntegerType()),
    StructField("start_count", IntegerType()),
    StructField("speed", DoubleType()),
    StructField("engine_quality", DoubleType()),
    StructField("fuel_level", DoubleType()),
    StructField("tire_quality", tire_schema),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("timestamp", TimestampType())
])

# 3. Kafka Source
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "vehicle_telemetry") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. Transformations
enriched_df = json_df \
    .withColumn("tire_avg", expr("(tire_quality.front_left + tire_quality.front_right + tire_quality.rear_left + tire_quality.rear_right) / 4")) \
    .withColumn("speed_category", when(col("speed") > 80, "High")
                                   .when((col("speed") > 40) & (col("speed") <= 80), "Medium")
                                   .otherwise("Low")) \
    .withColumn("fuel_alert", when(col("fuel_level") < 20, "Low Fuel").otherwise("OK")) \
    .withColumn("engine_status", when(col("engine_quality") >= 80, "Good")
                                 .when((col("engine_quality") >= 50) & (col("engine_quality") < 80), "Moderate")
                                 .otherwise("Poor")) \
    .withColumn("vehicle_age", year(current_timestamp()) - col("year")) \
    .withColumn("car_quality_score", expr("ROUND((engine_quality * 0.5 + tire_avg * 0.4 - vehicle_age * 1.5), 2)")) \
    .withColumn("start_usage", when(col("start_count") > 10, "Frequently Used").otherwise("Normal"))

# 5. Write to Elasticsearch
def write_to_elasticsearch(batch_df, batch_id):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "vehicle_data") \
        .mode("append") \
        .save()

query = enriched_df.writeStream \
    .foreachBatch(write_to_elasticsearch) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()
