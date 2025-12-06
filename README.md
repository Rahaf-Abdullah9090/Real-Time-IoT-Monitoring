Real-Time IoT Monitoring Pipeline

MQTT → Kafka → Spark → InfluxDB → Grafana

This project implements a complete real-time data processing pipeline designed for IoT sensor analytics. It ingests live sensor readings, streams them through Kafka, processes them using Spark Structured Streaming, stores them in InfluxDB, and visualizes them via Grafana dashboards and alert rules.

 System Architecture

MQTT Sensor Simulator: Publishes voltage, current, and power readings.

Kafka Broker: Buffers messages in topic sensor-data.

Spark Structured Streaming: Parses JSON, transforms records, and writes them to InfluxDB.

InfluxDB: Stores time-series data in bucket iot_data.

Grafana: Visualizes metrics and triggers alerts.

 Repository Structure
/
|-- docker-compose.yml
|-- spark/
|     |-- spark_streaming.py
|
|-- modbus-simulator/
|     |-- mqtt_publisher.py
|
|-- config/
|     |-- mosquitto.conf
|     |-- telegraf.conf
|
|-- screenshots/
|     |-- architecture.png
|     |-- mqtt_logs.png
|     |-- kafka_stream.png
|     |-- spark_logs.png
|     |-- influxdb_graph.png
|     |-- grafana_dashboard.png
|     |-- grafana_alerts.png
|
|-- README.md

 How to Run the Pipeline
1. Start all services
docker compose up -d

2. View Spark logs
docker logs spark --follow

3. Test Kafka consumer
kafka-console-consumer --bootstrap-server kafka:9092 --topic sensor-data

4. Open InfluxDB

URL: http://localhost:8086

Bucket: iot_data

5. Open Grafana

URL: http://localhost:4000

View dashboard + alerts.

Dashboard and Alerts

Grafana visualizes:

Voltage (V_L1)

Current (I_L1)

Active Power (P_L1)

Apparent Power (VA_L1)

Alerts notify when values fall outside thresholds.
