# 🚢 Anomaly Detection Dashboard

A multi-layered pipeline designed to detect financial, logistical, and classification anomalies in shipment data. The system combines hard-coded business logic, statistical outlier detection, and LLM-powered validation to ensure high precision in identifying discrepancies.

## 📊 Overview

This project provides a comprehensive solution for monitoring trade shipments. It processes synthetic shipment data through three specialized detection layers and presents the results via a real-time Streamlit dashboard.

### Detection Layers

1. **Layer 1: Rule Engine** – Executes 8 deterministic business rules (e.g., price mismatches, Incoterm violations, and invalid HS code formats).
2. **Layer 2: Statistical Detector** – Uses the **Interquartile Range (IQR)** method to identify outliers in pricing, transit times, and freight costs, as well as shifts in buyer behavior.
3. **Layer 3: LLM Detector** – Employs Large Language Models (LLMs) to perform high-precision HS code validation, specifically checking for mismatches between product descriptions and trade codes.

## 📂 Project Structure

* `main.py`: The primary orchestration script that runs the data generation, detection pipeline, and reporting.
* `app.py`: A Streamlit-based dashboard for visualizing anomalies, tracking KPIs, and managing the analysis pipeline.
* `data_generator.py`: Generates synthetic shipment, buyer, and product data, including "planted" anomalies for testing.
* `rule_engine.py`: Contains deterministic business logic checks.
* `statistical_detector.py`: Implements IQR-based statistical anomaly detection.
* `llm_detector.py`: Optimized LLM-based verification for complex classification checks.
* `generate_accuracy_report.py`: Compares detected anomalies against planted ones to generate precision and recall metrics.

## 🚀 Getting Started

### Prerequisites

* Python 3.8+
* Required libraries: `streamlit`, `pandas`, `plotly`, `numpy`, `groq` (for LLM layer).

### Installation & Execution

1. **Install Dependencies**:
```bash
pip install streamlit pandas plotly numpy groq

```


2. **Run the Pipeline**:
Execute the orchestration script to generate data and run detection layers:
```bash
python main.py

```


*Note: This will create a `data/` directory with CSV files and an `output/` directory with JSON reports.*
3. **Launch the Dashboard**:
View the results interactively:
```bash
url : https://trade-anomaly-3gm28lvrt9prfvajdxentz.streamlit.app/

```



## 📈 Performance Tracking

The system includes an accuracy reporting tool (`generate_accuracy_report.py`) that calculates:

* **Precision**: The accuracy of the flags raised.
* **Recall**: The ability to find all planted anomalies.
* **F1 Score**: The balanced metric of system performance.

Detected anomalies are saved to `output/anomaly_report.json` for integration with other business intelligence tools.