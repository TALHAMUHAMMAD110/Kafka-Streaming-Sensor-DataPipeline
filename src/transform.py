from pyspark.sql.functions import from_json, col, window, avg, timestamp_millis,round
from schema import SENSOR_SCHEMA

def parse_and_aggregate(spark, broker, topic):
    
    """
    Reads sensor data from a Kafka topic, parses JSON messages, computes 1-minute
    average values per sensor, and prepares the result for downstream sinks (Kafka, MongoDB).

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session.
    broker : str
        Kafka bootstrap servers, e.g., "broker:29092".
    topic : str
        Input Kafka topic name containing sensor data.

    Returns:
    --------
    pyspark.sql.DataFrame
        Aggregated stream with schema:
        sensorId, windowStart (ms), windowEnd (ms), averageValue
    """
    
    # 1. Read raw data from Kafka
    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Convert binary Kafka 'value' to string, then parse JSON using the predefined schema.
    parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
                .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data")) \
                .select("data.*") \
                .withColumn("eventTime", timestamp_millis(col("timestamp")))
                

    # Compute 1-minute tumbling window average per sensor.
    # Watermark is set to 1 minute to allow late events within 1 minute.
    aggregated = parsed.withWatermark("eventTime", "1 minutes") \
                       .groupBy(col("sensorId"), window(col("eventTime"), "1 minute")) \
                       .agg(round(avg("value"), 2).alias("averageValue"))
                       

    # Convert window start/end to milliseconds and select relevant columns for downstream processing.
    return aggregated.select(
        col("sensorId"),
        (col("window.start").cast("long") * 1000).alias("windowStart"),
        (col("window.end").cast("long") * 1000).alias("windowEnd"),
        col("averageValue")
    )