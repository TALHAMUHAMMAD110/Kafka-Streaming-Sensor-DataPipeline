# Sensor Data Streaming Pipeline

Real-time sensor data streaming pipeline using **PySpark Structured Streaming** and **Kafka**.

## Overview

This project ingests sensor data from a Kafka topic, performs real-time aggregation, and writes the results to another Kafka topic.

**Features:**

- Reads sensor data from Kafka topic: `sensor-input`
- Aggregates sensor readings in **1-minute windows**
- Computes the **average value** for each sensor per minute
- Writes aggregated results to Kafka topic: `sensor-output`
- Supports unit testing for transformations
- Modular design with separate transformation and sink modules

---

## Architecture

Kafka : To persist the incoming streaming messages and deliver to spark application

Spark: Structured Streaming to process the data from kafka, aggregating data using Data Frames. (Spark-SQL).

Spark Structured Steaming API: For writing out the data streams to RDBMS/ NoSQL databases/datawarehouse like Hive/S3.

![Kafka-Spark Architecture](./ArchitectureDiagram.png "Architecture")

## Prepare your development environment

- Install Docker in your local machine
- Run Kafka and Kafka Producer

'start' Run following commands in your terminal.

```
docker-compose up --build

```

- Wait for 2-3 minutes until all of the docker containers are running.
