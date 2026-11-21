# ⚡ FlowPulse AI — Greater Bengaluru Kinetic Digital Twin

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming%20%26%20MLlib-E25A1C?logo=apachespark&logoColor=white)
![Apache Cassandra](https://img.shields.io/badge/Apache%20Cassandra-Time--Series-1287B1?logo=apachecassandra&logoColor=white)
![Status](https://img.shields.io/badge/Coverage-32%20Hubs%20%7C%206%20Zones-10B981)

**FlowPulse AI** is an end-to-end Big Data urban traffic intelligence platform and kinetic digital twin built for **Greater Bengaluru**. It ingests live telemetry from **6,414 inductive loop sensors and BMTC GPS probes** across **32 major junctions** and **54 arterial corridors**, predicting `+30-minute` corridor velocities and congestion shockwaves using **Apache Kafka**, **Apache Spark Structured Streaming**, **Spark MLlib (GBTRegressor)**, and **Apache Cassandra**.

---

## ✨ Key Features

- **🌐 6-Zone Kinetic Geo-Command (Google Maps Engine):** Real-time spatial tracking across **Central, North, East IT, South, West, and Outer Belt** zones with live **Namma Metro** overlays (Purple, Green, Yellow, and Blue Airport lines).
- **⚡ Kinetic Corridor Flows & Bottleneck Sonar:** Dynamic SVG velocity streams scaled by live corridor speed (`km/h`), paired with expanding sonar shockwaves around congested junctions (`<20 km/h`) and active incident zones.
- **🧭 Dijkstra + Spark ML Route Planner:** Computes minimum-time paths between any two Bengaluru hubs using live speeds alongside `+30m` ML-predicted travel times.
- **🎛️ Spark MLlib What-If Simulator:** Interactive Gradient Boosted Trees (`GBTRegressor`, $R^2 = 96.8\%$) simulator with SHAP feature weight breakdowns for peak rush hours, vehicle density, monsoon waterlogging, and lane blockages.
- **📊 All-Bengaluru Analytics & 18-Hour Heatmap:** 6-zone velocity benchmarks, spatio-temporal congestion matrices, and a sortable 32-hub real-time leaderboard.
- **⌘ Command Palette (`Ctrl + K`) & 5-Stage Pipeline Inspector:** Instant keyboard navigation across all 32 hubs plus interactive blueprints for Kafka, Spark, and Cassandra schemas.

---

## 🏗️ Big Data Architecture

```text
[6,414 IoT Loops & BMTC GPS] 
        │  (2s JSON telemetry packets)
        ▼
[Apache Kafka Broker] ── Topic: bengaluru.traffic.telemetry (12 Zone Partitions)
        │
        ▼
[Apache Spark Structured Streaming] ── 5-min Sliding Windows + 2-min Event Watermarks
        │
        ├──► [Apache Cassandra] (Low-latency hot reads by zone, hub & date)
        ├──► [HDFS Parquet]     (Historical spatio-temporal archival)
        └──► [Spark MLlib GBT]  (+30-minute velocity & congestion inference)
                │
                ▼
[FlowPulse AI Kinetic Command Deck] (127.0.0.1:8000)
