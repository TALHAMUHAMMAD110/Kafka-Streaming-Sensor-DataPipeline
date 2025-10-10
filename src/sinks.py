def write_to_kafka(df, broker, topic, checkpoint):
    df.selectExpr("CAST(sensorId AS STRING) AS key", "to_json(struct(*)) AS value") \
      .writeStream \
      .outputMode("append") \
      .format("kafka") \
      .option("kafka.bootstrap.servers", broker) \
      .option("topic", topic) \
      .option("checkpointLocation", checkpoint) \
      .start()

def write_to_mongo(df, uri, db, collection, checkpoint):
    df.writeStream \
      .outputMode("append") \
      .format("mongodb") \
      .option("spark.mongodb.connection.uri", uri) \
      .option("spark.mongodb.database", db) \
      .option("spark.mongodb.collection", collection) \
      .option("checkpointLocation", checkpoint) \
      .start()

