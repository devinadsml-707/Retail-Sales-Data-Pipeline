# 🛍️ Retail Sales Performance & Revenue Analysis Across Shopping Malls

<div align="center">

![Pipeline Status](https://img.shields.io/badge/Pipeline-Passing-brightgreen?style=for-the-badge&logo=apache-airflow)
![Python](https://img.shields.io/badge/Python-3.10-blue?style=for-the-badge&logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=for-the-badge&logo=apache-airflow)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.x-005571?style=for-the-badge&logo=elasticsearch)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-316192?style=for-the-badge&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker)

**An end-to-end automated ETL pipeline for retail analytics — from raw transactional data in PostgreSQL to a searchable Elasticsearch index, orchestrated via Apache Airflow and validated with Great Expectations.**

[Problem Background](#-problem-background) · [Architecture](#-architecture) · [Pipeline](#-etl-pipeline) · [Setup](#-local-setup) · [Results](#-results--insights) · [Stack](#-tech-stack)

</div>

---

## 📌 Problem Background

Retail shopping mall management often struggles to make **data-driven decisions** due to a lack of consolidated sales insights. Without a clear understanding of:
- Revenue trends across mall locations
- Top-performing product categories
- Customer purchasing patterns by demographic
- Payment method preferences

...businesses risk **misallocating marketing resources**, missing peak sales opportunities, and failing to optimize product and payment strategies.

This project addresses these challenges by building a **comprehensive ETL pipeline** that automatically extracts, cleans, and loads retail transactional data into an analytics-ready NoSQL store (Elasticsearch), enabling real-time business intelligence via Kibana dashboards.

---

## 🎯 Objectives

| # | Business Question |
|---|---|
| 1 | Which mall locations are top/bottom performers, and how large is the revenue gap? |
| 2 | Which product categories drive the most revenue, and which are underperforming? |
| 3 | What are the dominant payment methods, and is there an opportunity to shift toward cashless? |
| 4 | How do monthly sales trends behave across mall locations — any seasonal patterns? |
| 5 | How do spending patterns differ across age groups and gender? |
| 6 | Which gender × age group combinations contribute the most to total revenue? |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow (Orchestration)               │
│   Schedule: Every Saturday at 09:10, 09:20, 09:30 UTC               │
│                                                                     │
│  ┌──────────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │  fetch_from_     │──▶│  data_cleaning   │──▶│  post_to_      │  │
│  │  postgresql      │   │                  │   │  elasticsearch │  │
│  └──────────────────┘   └──────────────────┘   └────────────────┘  │
│          │                       │                      │           │
└──────────┼───────────────────────┼──────────────────────┼───────────┘
           ▼                       ▼                      ▼
    ┌─────────────┐        ┌──────────────┐       ┌─────────────────┐
    │ PostgreSQL  │        │ Great        │       │ Elasticsearch   │
    │ (Source DB) │        │ Expectations │       │ + Kibana        │
    │ table_m3    │        │ (Validation) │       │ (Dashboard)     │
    └─────────────┘        └──────────────┘       └─────────────────┘
```

---

## 🔄 ETL Pipeline

The DAG (`retail_etl_dag.py`) runs **3 sequential tasks**:

### Task 1 — `fetch_from_postgresql`
- Connects to PostgreSQL (`milestone3` database)
- Reads all records from `table_m3` (99,457 rows × 10 columns)
- Saves raw data as `/tmp/retail_transactions_raw.csv`

### Task 2 — `data_cleaning`
Performs a full cleaning pipeline:

| Step | Action |
|------|--------|
| Column Normalization | Lowercase, strip whitespace, replace spaces with `_`, remove special chars |
| Duplicate Removal | Drops fully identical rows |
| Missing Value Imputation | Numerical → median; Categorical → mode |
| Type Fixing | `age` & `quantity` → `int`; `price` → `float (2dp)`; `invoice_date` → `datetime` |

Output saved to `/tmp/retail_transactions_clean.csv`

### Task 3 — `post_to_elasticsearch`
- Connects to Elasticsearch at `http://elasticsearch:9200`
- Bulk-indexes all cleaned records into index `retail_transactions`
- Reports success/failure counts

**DAG Run Summary (from Airflow UI):**
- ✅ Total Runs: 2 | Total Success: 2
- ⏱️ Mean Run Duration: **00:00:24**
- 📅 Last Run: 2026-03-29, 19:53:47 UTC

---

## 📊 Dataset

| Property | Detail |
|----------|--------|
| **Source** | [Kaggle — Sales and Customer Data](https://www.kaggle.com/datasets/dataceo/sales-and-customer-data) |
| **Original tables** | `customer_data.csv` + `sales_data.csv` (merged via SQL JOIN) |
| **Raw rows** | 99,457 |
| **Columns** | 10 |
| **Missing values** | 0 (after cleaning) |
| **Date range** | 2021 – 2023 |
| **Geography** | Multiple malls in Istanbul, Turkey |

### Column Descriptions

| Column | Type | Description |
|--------|------|-------------|
| `invoice_no` | `VARCHAR` | Unique invoice identifier |
| `customer_id` | `VARCHAR` | Customer identifier |
| `gender` | `VARCHAR` | Male / Female |
| `age` | `INT` | Customer age |
| `payment_method` | `VARCHAR` | Credit Card / Debit Card / Cash |
| `category` | `VARCHAR` | Product category (Clothing, Shoes, etc.) |
| `quantity` | `INT` | Number of items purchased |
| `price` | `FLOAT` | Unit price |
| `invoice_date` | `DATE` | Date of purchase |
| `shopping_mall` | `VARCHAR` | Mall location |

---

## ✅ Data Validation — Great Expectations

Data quality is enforced using **Great Expectations** before indexing. Expectations include:

```python
# Example expectations defined in notebooks/data_validation_gx.ipynb
expect_column_values_to_not_be_null("invoice_no")
expect_column_values_to_be_unique("invoice_no")
expect_column_values_to_be_between("age", min_value=18, max_value=100)
expect_column_values_to_be_between("quantity", min_value=1)
expect_column_values_to_be_between("price", min_value=0)
expect_column_values_to_be_in_set("gender", ["Male", "Female"])
expect_column_values_to_be_in_set("payment_method", ["Credit Card", "Debit Card", "Cash"])
```

---

## 🖥️ DAG Screenshots

<table>
  <tr>
    <td align="center"><b>DAG List (Airflow Home)</b></td>
    <td align="center"><b>DAG Graph View</b></td>
  </tr>
  <tr>
    <td><img src="images/dag_home.png" alt="DAG Home"/></td>
    <td><img src="images/dag_graph.png" alt="DAG Graph"/></td>
  </tr>
  <tr>
    <td align="center"><b>DAG Grid Run History</b></td>
    <td align="center"><b>DAG Run Summary</b></td>
  </tr>
  <tr>
    <td><img src="images/dag_grid.png" alt="DAG Grid"/></td>
    <td><img src="images/dag_summary.png" alt="DAG Summary"/></td>
  </tr>
</table>

---

## 🚀 Local Setup

> ⚠️ This project runs fully locally via Docker Compose. No cloud costs required.

### Prerequisites
- Docker Desktop installed and running
- Python 3.10+
- 4GB+ RAM recommended for Docker

### Steps

**1. Clone the repository**
```bash
git clone https://github.com/your-username/p2-m3-retail-pipeline.git
cd p2-m3-retail-pipeline
```

**2. Start all services via Docker Compose**
```bash
docker-compose up -d
```
This starts: PostgreSQL, Apache Airflow (webserver + scheduler), and Elasticsearch + Kibana.

**3. Load raw data into PostgreSQL**

Run the setup script to create `table_m3` and insert the raw CSV:
```bash
python scripts/load_to_postgres.py
```
Or apply the schema directly via psql using `scripts/db_schema.sql`.

**4. Access Airflow UI**
```
URL      : http://localhost:8080
Username : airflow
Password : airflow
```
Enable the `retail_etl_pipeline` DAG and trigger it manually (▶ button).

**5. Access Kibana Dashboard**
```
URL : http://localhost:5601
```
Create an index pattern for `retail_transactions` to start visualizing.

### Service Ports

| Service | Port |
|---------|------|
| Airflow Webserver | `8080` |
| PostgreSQL | `5434` |
| Elasticsearch | `9200` |
| Kibana | `5601` |

---

## 📈 Results & Insights

> Analysis performed on 99,457 retail transactions across shopping malls in Istanbul.

### Key Findings
- 💰 **Revenue leaders**: Mall of Istanbul and Kanyon consistently lead in total revenue
- 👗 **Top category**: Clothing dominates sales volume across all malls
- 💳 **Payment split**: Cash, Credit Card, and Debit Card each hold ~33% share — significant cashless opportunity
- 📅 **Seasonality**: Q4 (Oct–Dec) shows peak sales; summer months show a consistent dip
- 👥 **High-value segment**: Female customers aged 26–50 contribute the highest total spend
- 📊 **Age group 36–50**: Highest average transaction value across all categories

---

## 🛠️ Tech Stack

| Layer | Tools |
|-------|-------|
| **Orchestration** | Apache Airflow 2.x |
| **Source Database** | PostgreSQL 15 |
| **Analytics Store** | Elasticsearch 8.x |
| **Visualization** | Kibana |
| **Data Validation** | Great Expectations |
| **Data Processing** | Python, Pandas, NumPy |
| **Containerization** | Docker, Docker Compose |
| **DB Connector** | Psycopg2, SQLAlchemy |
| **Analysis & EDA** | Matplotlib, Seaborn, Jupyter Notebook |

---

## 📁 Repository Structure

```
retail-etl-pipeline/
│
├── README.md                               ← You are here
├── docker-compose.yml                      ← Spin up all services locally
├── requirements.txt                        ← Python dependencies
├── .gitignore
│
├── dags/
│   └── retail_etl_dag.py                  ← Airflow DAG (fetch → clean → index)
│
├── notebooks/
│   └── data_validation_gx.ipynb           ← Great Expectations validation
│
├── scripts/
│   ├── load_to_postgres.py                ← Seeds raw data into PostgreSQL
│   └── db_schema.sql                      ← DDL for table_m3
│
├── data/
│   ├── raw/
│   │   └── retail_transactions_raw.csv    ← Raw merged dataset (99,457 rows)
│   └── processed/
│       └── retail_transactions_clean.csv  ← Cleaned, pipeline-ready dataset
│
└── images/
    ├── dag_home.png
    ├── dag_graph.png
    ├── dag_grid.png
    └── dag_summary.png
```

---

## 💡 Showing a Local-Only Project Professionally

Since this project runs locally, here are strategies used to demonstrate it professionally **without cloud costs**:

| Strategy | How |
|----------|-----|
| **Screenshots** | Airflow UI, Kibana dashboards, GX validation reports captured and committed |
| **Screen recording** | Record pipeline running end-to-end; upload to Google Drive / YouTube (unlisted) and link in README |
| **Docker Compose** | Anyone can clone and reproduce locally in minutes |
| **Data samples** | Raw & clean CSVs committed so reviewers can inspect the data directly |
| **Notebooks** | Jupyter notebooks with outputs preserved show analysis without needing to re-run |

---

## 👤 Author

**Devina Agustina**  
Batch: FTDS-052-RMT  
Phase 2 — Milestone 3  

---

## 📚 References

- [Kaggle Dataset — Sales and Customer Data](https://www.kaggle.com/datasets/dataceo/sales-and-customer-data)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Elasticsearch Documentation](https://www.elastic.co/guide/index.html)
- [McKinsey State of Fashion Report](https://www.mckinsey.com/industries/retail/our-insights/state-of-fashion-archive)
