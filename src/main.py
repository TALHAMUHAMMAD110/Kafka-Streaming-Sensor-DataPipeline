from pyspark.sql import SparkSession
from transform import parse_and_aggregate
from sinks import write_to_kafka, write_to_mongo
from config import *

# Initialize Spark
spark = SparkSession.builder.appName("KafkaToMongoStreaming").getOrCreate()

# Transform stream
df = parse_and_aggregate(spark, KAFKA_BROKER, KAFKA_INPUT_TOPIC)

# Write to Kafka and MongoDB
write_to_kafka(df, KAFKA_BROKER, KAFKA_OUTPUT_TOPIC, CHECKPOINT_KAFKA)
# write_to_mongo(df, MONGO_URI, MONGO_DB, MONGO_COLLECTION, CHECKPOINT_MONGO)

spark.streams.awaitAnyTermination()