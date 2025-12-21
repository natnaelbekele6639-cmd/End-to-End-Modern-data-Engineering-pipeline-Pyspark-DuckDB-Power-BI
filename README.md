# 🌍 Global Internet Connectivity ETL Pipeline

## 📌 Project Overview
This project designs and implements a scalable, resilient **ETL (Extract, Transform, Load) data pipeline** that processes large-scale global internet performance data and prepares it for advanced analytics and business intelligence reporting.

Using **Apache PySpark** for distributed processing, **DuckDB** for high-performance analytical querying, and **Prefect** for workflow orchestration, the pipeline integrates heterogeneous data sources to generate actionable insights into the global **Digital Divide**.

The final curated dataset is visualized using **Microsoft Power BI** to support data-driven decision-making related to internet infrastructure development and digital equity.

---

## 🎯 Business Problem
Reliable internet connectivity is a major driver of economic growth, education, and innovation. However, internet performance varies significantly across regions and countries.

This project addresses key analytical questions such as:

- **Global Disparity:** Which countries have the fastest and slowest internet infrastructure?
- **Population Impact:** How does population size correlate with internet speed and latency?
- **Infrastructure Quality:** What proportion of the global population experiences excellent versus poor connectivity?

---

## 🧩 Data Sources
The ETL pipeline integrates three distinct datasets:

| Source Name | Format | Description |
|------------|--------|-------------|
| Ookla Speedtest Intelligence® | Parquet | High-volume fixed broadband speed test data |
| Natural Earth Admin 0 | GeoJSON | World country boundaries for geospatial mapping |
| World Population Data | CSV | Country-level population figures (filtered to 2018) |

✔️ **Meets the requirement of using at least one Parquet data source**

---

## 🏗 Architecture Overview

---

## 🛠 ![orches](https://github.com/user-attachments/assets/0b7f18c4-1150-4c2c-908e-b9a06c063d48)

Technologies Used

| Category | Tool |
|--------|------|
| Distributed Processing | Apache PySpark |
| Orchestration | Prefect |
| Storage | Parquet (Staging Layer) |
| Analytics Database | DuckDB |
| BI & Visualization | Microsoft Power BI |
| Programming Language | Python 3.11+ |
| OS Support | Windows-safe (Custom Hadoop binaries) |

---

## 🔄 ETL Pipeline Description

### 1️⃣ Extract
- **Concurrency:** Uses `prefect.submit()` to download Parquet, GeoJSON, and CSV files in parallel
- **Smart Caching:** Hash-based checks prevent redundant downloads if data is unchanged

### 2️⃣ Transform (PySpark)
- **Data Cleaning:** Handles missing latency values using global averages; filters unreliable tiles
- **Standardization:** Converts internet speeds from Kbps to Mbps
- **Enrichment:** Maps Ookla quadkeys to countries and joins population data
- **Feature Engineering:** Generates a `Performance_Grade`  
  *(Excellent, Very Good, Good, Average, Poor)*

### 3️⃣ Load
- **Staging Layer:** Writes transformed data to Parquet for scalability
- **Warehouse Layer:** Loads data into DuckDB using `read_parquet`
- **BI Export:** Generates a CSV file optimized for Power BI

---

## ⏱️ Pipeline Orchestration (Prefect)
The entire ETL workflow is orchestrated using **Prefect**, providing:

- Task-level fault tolerance
- Parallel execution
- Automatic retries and caching

### Key Features
- **Parallel Extraction**
- **24-hour Task Caching**
- **Daily Scheduled Execution (Cron-based)**


### 📁 Repository Structure
<img width="502" height="385" alt="image" src="https://github.com/user-attachments/assets/81773883-84b4-4b06-a4a1-985e86be0c35" />


### 📊 Power BI Dashboard Preview
![photo_2025-12-16_02-08-40](https://github.com/user-attachments/assets/3e67d759-f0b8-40ae-868d-a88547488138)

### 👥 Team Members & Roles
| Name            | ID         | Role / Contribution                                  |
| --------------- | ---------- | ---------------------------------------------------- |
| Natnael Bekele  | DBU1501407 | Project Lead – Architecture & Pipeline Strategy      |
| Bereket Getaw   | DBU1501044 | BI Developer – Power BI Dashboard Design|
| Hafiz Husen     | DBU1501241 | Orchestration Lead – Prefect Deployment & Scheduling |
| Rediet Esubalew | DBU1501704 | Data Ingestion – Source Integration & Null Handling  |
| Dawit Alemu     | DBU1501117 | Database Admin – DuckDB Integration & Validation     |
| Yiferu Mekonen  | DBU1501562 | Data Quality – Validation & Performance Grading      |
| Elbetel Abedi   | DBU1501145 |  Data Engineer – Spark Transformations & Optimization              |
| Genet Minda     | DBU1501217 | Documentation – Reporting & Repository Management    |

**🚀 How to Run the Project**
1️⃣ Install Dependencies
   pip install pyspark duckdb prefect pandas requests
2️⃣ Start the Orchestration Server
   python orchestration/advanced_orchestration.py
3️⃣ Trigger the ETL Pipeline
    prefect deployment run "Advanced Global Pipeline/daily-etl-deployment"
4️⃣ Generate Power BI Dataset
    python orchestration/create_csv.py
## 📈 Key Metrics & Insights
    Global internet performance distribution by country

    Speed vs population correlation analysis

    Regional connectivity comparison (continents)

    Identification of underserved high-population regions
## 🔧 Technical Highlights
    Highly Scalable: Processes millions of records using distributed Spark execution

    Fault Tolerant: Task retries, caching, and error handling via Prefect

    Performance Optimized: Columnar storage and analytical query engine

    Cross-Platform: Windows-compatible Hadoop configuration included
