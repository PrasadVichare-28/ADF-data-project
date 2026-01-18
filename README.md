<p align="center">
  <img src="https://img.shields.io/badge/Cloud-Azure-0078D4?logo=microsoftazure&logoColor=white"/>
  <img src="https://img.shields.io/badge/Streaming-Kafka-231F20?logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Platform-Databricks-EF3E42?logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Storage-Delta%20Lake-003B57"/>
  <img src="https://img.shields.io/badge/Language-Python-3776AB?logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/ML-Production--Ready-success"/>
</p>

# 🚨 Real-Time Fraud Detection with Drift Monitoring (Azure | Streaming | ML)

<p align="center">
  <b>End-to-end, production-style fraud detection system</b><br/>
  Batch + Real-Time Streaming • Cloud-Scale • Drift-Aware Machine Learning
</p>

---

## 📌 Overview

Financial fraud patterns evolve continuously. A model that performs well today may silently degrade tomorrow if **data and behavior change**.

This project implements a **cloud-scale, real-time fraud detection system** that not only scores transactions in **batch and live streaming modes**, but also **monitors feature and prediction drift** to maintain long-term model reliability.

> 🎯 Focus: **Operational Machine Learning**, not just model accuracy.

---

## 🏗️ System Architecture

### 🔹 Key Design Principles
- Unified **batch + streaming** data pipelines  
- **Bronze–Silver–Gold** lakehouse architecture  
- Real-time ML inference with **low latency**  
- Built-in **drift detection** for production reliability  

### 🔹 Architecture Highlights
- **Batch Data** → Azure Data Factory → ADLS (Delta Lake)
- **Live Streaming Data** → Azure Event Hubs (Kafka API)
- **Processing & ML** → Azure Databricks (Spark Structured Streaming)
- **Storage** → ADLS Gen2 with Delta Lake (ACID, versioning, time travel)

📊 Architecture diagrams and detailed workflows are included in the `docs/overall` folder.

---

## 🔄 Data Flow Summary

| Flow | Description |
|----|------------|
| 🟢 **Initial Load** | Historical CSV → Training + Drift Baseline |
| 🔵 **Daily Incremental** | New CSV → Daily Batch Scoring |
| 🔴 **Live Streaming** | JSON Events → Real-Time Scoring + Alerts |

---

## 📊 Dataset & Simulator

A **custom Python transaction simulator** generates realistic financial data for both batch and streaming workflows.

### Transaction Attributes
- Transaction ID  
- Timestamp  
- Customer ID  
- Terminal ID  
- Amount  
- Customer & terminal latitude/longitude  
- Fraud label (binary)

### Fraud Injection Logic
Fraud is simulated using probabilistic rules such as:
- Unusual transaction times (late night)
- High geo-distance between customer and terminal
- Rapid transaction bursts
- High-risk terminals

---

## 🧠 Feature Engineering

Fraud signatures are captured using engineered features:

### 🌍 Geospatial
- Haversine distance between customer and terminal  

### ⏰ Temporal
- Hour of day, day of week, weekend indicator  

### ⚡ Velocity (Stateful Streaming)
- Transaction count in rolling windows (2-min, 5-min)  

### 👤 Behavioral
- Customer spend median, IQR, anomaly detection  

### 🏪 Terminal Risk
- Rolling terminal-level fraud rate  

> ⚙️ Streaming features are computed using **stateful Spark aggregations**.

---

## 🤖 Machine Learning

### Models Evaluated
- Logistic Regression  
- Random Forest  
- Gradient Boosted Trees  

### Metrics
- Precision @ FPR 0.05  
- AUC-ROC  
- AUC-PR  

### ✅ Selected Model
**Logistic Regression**
- Fast inference
- Stable under streaming workloads
- Strong precision performance

---

## ⚡ Real-Time Streaming Pipeline

1. Python Kafka producer emits live JSON transactions  
2. **Azure Event Hubs** ingests events (Kafka-compatible)  
3. **Databricks Structured Streaming**:
   - Parses & validates events
   - Computes real-time features
   - Loads and broadcasts ML model
   - Generates fraud probability
   - Creates binary fraud alerts  
4. Outputs written to Delta Lake:
   - `stream_scores` (all transactions)
   - `stream_alerts` (flagged fraud cases)

✔️ Exactly-once processing with checkpointing

---

## 📉 Drift Monitoring (PSI)

### Why Drift Matters
- Customer behavior changes
- Spending patterns shift
- Fraud strategies evolve

### Drift Detection Method
**Population Stability Index (PSI)** monitors:
- Feature distribution drift
- Prediction score drift

### PSI Thresholds
| PSI Value | Interpretation |
|--------|----------------|
| < 0.10 | No drift |
| 0.10 – 0.20 | Moderate drift |
| ≥ 0.20 | Severe drift → Retraining |

Drift events are logged into Delta tables for auditing and monitoring.

---

## 🔁 Retraining Control Logic

Model retraining is flagged when:
- PSI exceeds severe drift threshold **OR**
- Precision drops below acceptable level (e.g., < 0.60)

Control signals are stored in a model control table to coordinate retraining decisions.

---

## 🗂️ Delta Lake Architecture

### 🥉 Bronze
- Raw batch data
- Raw streaming events
- Streaming checkpoints

### 🥈 Silver
- Cleaned and validated data
- Parsed schemas
- Enriched fields

### 🥇 Gold
- Feature-engineered datasets
- Model artifacts
- Drift baselines and events
- Scored outputs and alerts
---

## ▶️ How to Run

### Prerequisites
- Azure Subscription  
- Azure Databricks Workspace  
- Azure Event Hubs (Kafka enabled)  
- ADLS Gen2  
- Python 3.9+  

### High-Level Steps
1. Generate batch CSV data using simulator  
2. Run ADF initial load pipeline  
3. Execute Databricks batch notebooks (training + baseline)  
4. Start Kafka producer for live streaming  
5. Run Databricks streaming notebook  
6. Monitor scoring, alerts, and drift events  

---

## 🚀 Future Enhancements
- Additional drift detectors (ADWIN, KL divergence)
- Automated retraining with CI/CD
- Real-time dashboards (Databricks SQL / Power BI)
- Ensemble fraud detection models
- Alert routing via Azure Functions / Logic Apps

---

## 👤 Author

**Prasad Vichare**  
MS in Management Information Systems  
University of Illinois Chicago  

🔗 LinkedIn: https://www.linkedin.com/in/prasad-vichare  

---


