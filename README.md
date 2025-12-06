# 🚀 Real-Time IoT Data Pipeline | Confluent MQTT

> **Production-ready streaming data architecture** showcasing modern data engineering practices with **Apache Spark**, **MQTT**, **InfluxDB**, and **Docker**

## 🎯 Project Highlights

**End-to-End Data Engineering Solution** demonstrating:
- ⚡ **Real-time streaming** with Apache Spark & MQTT
- 🏭 **Industrial IoT simulation** using Modbus protocol
- 📊 **Time-series data storage** with InfluxDB
- 🐳 **Containerized microservices** architecture
- 📈 **Scalable data processing** pipeline

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|------------|----------|
| **Streaming Engine** | Apache Spark | Real-time data processing |
| **Message Broker** | MQTT (Mosquitto) | IoT device communication |
| **Time-Series DB** | InfluxDB | High-performance data storage |
| **Data Collection** | Telegraf | Metrics aggregation |
| **Orchestration** | Docker Compose | Service management |
| **Protocol Simulation** | Modbus | Industrial device emulation |

## 🏗️ Architecture Overview

```
[Modbus Devices] → [MQTT Broker] → [Spark Streaming] → [InfluxDB] → [Analytics]
       ↓              ↓              ↓              ↓
   Simulated      Message Queue   Real-time      Time-series
   Industrial     & Routing       Processing     Storage
   Data
```

## 🚀 Quick Start

**One-command deployment:**
```bash
docker-compose up -d
```

**What gets deployed:**
- MQTT broker with custom configuration
- Modbus device simulators generating realistic IoT data
- Spark streaming application processing data in real-time
- InfluxDB instance for time-series storage
- Telegraf for metrics collection

## 📁 Project Structure

```
confluent-mqtt/
├── modbus-simulator/     # Industrial device simulation
├── mqtt-forwarder/       # MQTT message routing
├── spark/               # Real-time data processing
├── New folder/          # Jupyter development notebooks
├── docker-compose.yml   # Service orchestration
├── mosquitto.conf      # MQTT broker config
└── telegraf.conf       # Metrics collection config
```

## 💡 Key Features

- **Scalable Architecture**: Microservices design for horizontal scaling
- **Real-time Processing**: Sub-second data processing with Spark Streaming
- **Industrial Standards**: Modbus protocol implementation
- **Production Ready**: Docker containerization with proper configurations
- **Monitoring Ready**: Built-in metrics collection with Telegraf
- **Development Friendly**: Jupyter notebooks for experimentation

## 🎯 Perfect for Data Engineers who want to see:
- Modern streaming data architectures
- IoT data pipeline implementation
- Docker-based service orchestration
- Real-time analytics capabilities
- Industrial protocol integration

---
*This project demonstrates practical experience with production-grade data engineering tools and real-time streaming architectures.*