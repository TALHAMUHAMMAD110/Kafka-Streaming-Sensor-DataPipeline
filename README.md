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

## Project Structure
