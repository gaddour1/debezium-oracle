# ⚡ Debezium Oracle CDC Pipeline

> End-to-end **Change Data Capture (CDC)** pipeline for real-time data streaming from Oracle to a modern data lakehouse.

### 🏗️ Architecture

```text
Oracle → Debezium / LogMiner → Kafka → Spark Structured Streaming
                                      ↓
                              Apache Iceberg → MinIO
                                      ↓
                              Prometheus → Grafana(Monitoring)
```

### 🛠️ Tech Stack

`Oracle` · `Debezium` · `Kafka` · `Spark` · `PySpark` · `Iceberg` · `MinIO` · `Prometheus` · `Grafana` · `Docker` · `Python`

### ✨ Features

* 🔄 Oracle **CDC** — INSERT / UPDATE / DELETE
* ⚡ Real-time data streaming with **Kafka**
* 🚀 Stream processing with **Spark Structured Streaming**
* 🗃️ Lakehouse storage with **Apache Iceberg**
* 📊 Monitoring with **Prometheus & Grafana**
* 🧪 Data validation & stress testing

### 📈 Results

**10K+** transactions tested · **137 rows/s** · **~5.8s** average processing latency

### 👤 Author

**Mohamed Amine Gaddour**
Junior Data Engineer · BI Developer · ERP & Data

[LinkedIn](https://linkedin.com/in/mohammedaminegaddour)
