from pyspark.sql.functions import from_json, col, window, avg, timestamp_millis,round
from schema import SENSOR_SCHEMA

def parse_and_aggregate(spark, broker, topic):
    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
                .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data")) \
                .select("data.*") \
                .withColumn("eventTime", timestamp_millis(col("timestamp")))

    aggregated = parsed.withWatermark("eventTime", "1 minutes") \
                       .groupBy(col("sensorId"), window(col("eventTime"), "1 minute")) \
                       .agg(round(avg("value"), 2).alias("averageValue"))


    return aggregated.select(
        col("sensorId"),
        (col("window.start").cast("long") * 1000).alias("windowStart"),
        (col("window.end").cast("long") * 1000).alias("windowEnd"),
        col("averageValue")
    )