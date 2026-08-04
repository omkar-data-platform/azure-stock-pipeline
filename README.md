"# azure-stock-pipeline" 

# ⚡ Real-Time Stock Price Analytics Pipeline

> A production-grade, end-to-end data engineering pipeline that ingests live stock market data, processes it using distributed computing, detects price anomalies, and serves insights through an interactive dashboard — built entirely on Azure.

<br/>

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)](https://delta.io)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

<br/>

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Pipeline Design](#-pipeline-design)
- [Project Structure](#-project-structure)
- [Key Features](#-key-features)
- [Data Model](#-data-model)
- [Financial Metrics](#-financial-metrics)
- [Delta Lake Best Practices](#-delta-lake-best-practices)
- [Orchestration](#-orchestration)
- [Results & Insights](#-results--insights)
- [Getting Started](#-getting-started)
- [Architecture Decisions](#-architecture-decisions)
- [Cost Analysis](#-cost-analysis)
- [What I Learned](#-what-i-learned)
- [Roadmap](#-roadmap)
- [Author](#-author)

<br/>

---

## 🎯 Overview

This project simulates a **real-world fintech data pipeline** — the kind built at trading firms, banks, and financial platforms to monitor live market activity.

### The Problem It Solves

Financial institutions need to:
- Monitor stock prices in **real-time** across multiple symbols
- Detect **unusual price movements** before they cause risk exposure
- Compute **industry-standard metrics** (VWAP, moving averages) at scale
- Maintain a **complete audit trail** of every data point ever processed
- Run **automated daily analytics** without manual intervention

This pipeline solves all of the above using a modern **Azure Lakehouse architecture**.

### What Makes This Production-Grade

| Concern | How It's Addressed |
|---|---|
| Fault tolerance | PySpark Structured Streaming with checkpointing |
| Late data handling | Watermark-based event-time processing |
| Data versioning | Delta Lake with 146+ tracked versions |
| Schema evolution | `mergeSchema` enabled on all Delta writes |
| Idempotency | `overwrite` mode on batch jobs — safe to re-run |
| Observability | Airflow task monitoring + Delta Lake history |
| Cost efficiency | Auto-terminating clusters + partitioned tables |

<br/>

---

## 🏗️ Architecture

```
                        ┌─────────────────────────────────────────────────────────┐
                        │                    AZURE CLOUD                          │
                        │                                                         │
  ┌─────────────┐       │  ┌──────────────┐     ┌──────────────────────────────┐ │
  │ Alpha       │       │  │    Azure     │     │     Azure Databricks          │ │
  │ Vantage API │──────▶│  │  Event Hubs  │────▶│                              │ │
  │             │       │  │              │     │  ┌────────────────────────┐  │ │
  │ (5 symbols  │       │  │  Kafka-compat│     │  │  PySpark Structured    │  │ │
  │  every 60s) │       │  │  message bus │     │  │  Streaming             │  │ │
  └─────────────┘       │  └──────────────┘     │  └──────────┬─────────────┘  │ │
         ▲              │                        │             │                 │ │
         │              │                        │             ▼                 │ │
  ┌─────────────┐       │                        │  ┌────────────────────────┐  │ │
  │  Python     │       │                        │  │   Delta Lake           │  │ │
  │  Producer   │       │                        │  │   Raw Table            │  │ │
  │  Script     │       │                        │  │   (98+ events)         │  │ │
  └─────────────┘       │                        │  └──────────┬─────────────┘  │ │
                        │                        │             │                 │ │
                        │                        │    Airflow triggers daily     │ │
                        │                        │             │                 │ │
                        │                        │             ▼                 │ │
                        │                        │  ┌────────────────────────┐  │ │
                        │                        │  │  PySpark Batch Jobs    │  │ │
                        │                        │  │  ├── VWAP              │  │ │
                        │                        │  │  ├── Moving Averages   │  │ │
                        │                        │  │  ├── Spike Detection   │  │ │
                        │                        │  │  └── OPTIMIZE/VACUUM   │  │ │
                        │                        │  └──────────┬─────────────┘  │ │
                        │                        └─────────────│─────────────────┘ │
                        │                                      │                   │
                        │            ┌─────────────────────────┘                   │
                        │            ▼                                             │
                        │  ┌──────────────────────────────────────────────────┐   │
                        │  │           Azure Data Lake Gen2                   │   │
                        │  │                                                  │   │
                        │  │  raw/stock_prices/          (Delta — partitioned)│   │
                        │  │  aggregated/vwap/            (Delta)             │   │
                        │  │  aggregated/moving_avg/      (Delta)             │   │
                        │  │  aggregated/price_spikes/    (Delta)             │   │
                        │  └──────────────────────┬───────────────────────────┘   │
                        │                         │                               │
                        └─────────────────────────│───────────────────────────────┘
                                                  │
                                                  ▼
                                    ┌─────────────────────────┐
                                    │    Power BI Dashboard   │
                                    │  Live prices, VWAP,     │
                                    │  Moving Avg, Spikes     │
                                    └─────────────────────────┘
```

<br/>

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Data Source** | Alpha Vantage API | Real-time stock prices |
| **Ingestion** | Python 3.10, `azure-eventhub` | Producer script |
| **Message Broker** | Azure Event Hubs | Kafka-compatible streaming bus |
| **Processing — Stream** | PySpark Structured Streaming | Real-time ingestion to Delta Lake |
| **Processing — Batch** | PySpark (Azure Databricks) | VWAP, MA, spike detection |
| **Storage** | Azure Data Lake Gen2 (ADLS) | Raw & processed data storage |
| **Table Format** | Delta Lake | ACID, time travel, versioning |
| **Orchestration** | Apache Airflow (Docker) | Daily batch job scheduling |
| **Visualization** | Power BI | Interactive dashboard |
| **CI/CD** | GitHub Actions | Linting, DAG validation |
| **IaC** | Docker Compose | Airflow infrastructure |

<br/>

---

## 🔄 Pipeline Design

This pipeline follows the **ELT (Extract → Load → Transform)** pattern with a **Lambda Architecture**:

```
┌─────────────────────────────────────────────────────────────────┐
│                      LAMBDA ARCHITECTURE                        │
│                                                                 │
│  SPEED LAYER (real-time)                                        │
│  ─────────────────────────────────────────────────────────────  │
│  Event Hubs → PySpark Streaming → Delta Raw Table               │
│  Latency: ~30 seconds                                           │
│                                                                 │
│  BATCH LAYER (scheduled)                                        │
│  ─────────────────────────────────────────────────────────────  │
│  Delta Raw Table → PySpark Batch Jobs → Delta Aggregated Tables │
│  Latency: runs daily at 6:00 AM via Airflow                     │
│                                                                 │
│  SERVING LAYER                                                  │
│  ─────────────────────────────────────────────────────────────  │
│  Delta Aggregated Tables → Power BI Dashboard                   │
└─────────────────────────────────────────────────────────────────┘
```

### Why ELT over ETL?

Traditional ETL transforms data **before** loading — this creates bottlenecks. This pipeline loads raw data into Delta Lake **first**, then transforms it **inside** Databricks using the full power of distributed computing. This is the modern lakehouse approach used by companies like Databricks, Snowflake, and dbt.

<br/>

---

## 📁 Project Structure

```
azure-stock-pipeline/
│
├── ingestion/
│   ├── producer.py              # Python script — polls API, pushes to Event Hubs
│   └── requirements.txt         # Python dependencies
│
├── databricks/
│   ├── notebooks/
│   │   ├── mount_storage.py     # Configure ADLS access from Databricks
│   │   ├── streaming_job.py     # PySpark Structured Streaming (9 cells)
│   │   ├── batch_jobs.py        # VWAP, Moving Avg, Spike Detection
│   │   └── delta_best_practices.py  # Partitioning, OPTIMIZE, VACUUM, Time Travel
│   │
│   └── jobs/
│       ├── vwap_job.py          # Standalone VWAP batch job (Airflow-triggered)
│       ├── moving_avg_job.py    # Standalone moving average job
│       ├── spike_detection_job.py  # Standalone spike detection job
│       └── optimize_job.py      # OPTIMIZE + VACUUM job
│
├── airflow/
│   ├── dags/
│   │   └── stock_pipeline_dag.py  # Main DAG — 6 tasks, runs daily at 6 AM
│   ├── docker-compose.yaml      # Airflow + Postgres via Docker
│   └── requirements.txt         # Airflow providers
│
├── docs/
│   └── architecture.png         # Architecture diagram
│
├── .github/
│   └── workflows/
│       └── pipeline_tests.yml   # CI/CD — lint + DAG validation
│
├── .gitignore
├── requirements.txt
└── README.md
```

<br/>

---

## ✨ Key Features

### 1. Real-Time Stock Ingestion
- Polls **5 stock symbols** (AAPL, MSFT, GOOGL, AMZN, TSLA) every 60 seconds
- Adds **±0.5% price simulation** to generate realistic price fluctuations
- Pushes structured JSON events to **Azure Event Hubs**
- Handles API errors gracefully with try/except per symbol

### 2. PySpark Structured Streaming
- Reads from Event Hubs using the **Kafka-compatible connector**
- Parses JSON payloads against a defined **StructType schema**
- Filters malformed records (`price > 0`, `symbol != null`)
- Writes to Delta Lake with **30-second micro-batch triggers**
- Checkpointing ensures **exactly-once processing**

### 3. Delta Lake Raw Table
- **ACID transactions** — no partial writes
- **Schema enforcement** — rejects events with wrong types
- **146+ versions** tracked — full audit trail since first write
- Partitioned by **date** and **symbol** for fast query performance

### 4. Advanced PySpark Batch Jobs
Three production-grade financial calculations run on the raw table:

- **VWAP** — Volume Weighted Average Price per symbol per day
- **Moving Averages** — MA3 and MA7 for trend signals
- **Price Spike Detection** — flags deviations > 1% from rolling average with HIGH/MEDIUM/LOW severity

### 5. Delta Lake Best Practices
- **Partitioning** by date + symbol → partition pruning reduces scan from 98 rows to 10
- **OPTIMIZE** → compacts small streaming files into 1 large file per table
- **VACUUM** → removes files older than 7 days, reducing storage cost
- **Time Travel** → query data at any historical version

### 6. Airflow Orchestration
- DAG: `stock_pipeline_daily` runs every day at **6:00 AM**
- 3 batch jobs run **in parallel** (VWAP, Moving Avg, Spike Detection)
- OPTIMIZE/VACUUM runs **after** all 3 complete
- Data quality checks validate row counts before pipeline marks success
- **2 automatic retries** with 5-minute delay on failure

<br/>

---

## 🗄️ Data Model

### Raw Table — `stock_prices`
```
Partitioned by: date, symbol

┌──────────────────┬───────────────┬─────────────────────────────┐
│ Column           │ Type          │ Description                 │
├──────────────────┼───────────────┼─────────────────────────────┤
│ symbol           │ STRING        │ Stock ticker (AAPL etc.)    │
│ price            │ FLOAT         │ Current stock price (USD)   │
│ volume           │ INTEGER       │ Shares traded today         │
│ change_percent   │ STRING        │ % change from previous close│
│ timestamp        │ TIMESTAMP     │ Event time (UTC)            │
│ date             │ DATE          │ Partition column            │
└──────────────────┴───────────────┴─────────────────────────────┘
```

### VWAP Table — `vwap`
```
┌──────────────────┬───────────────┬─────────────────────────────┐
│ Column           │ Type          │ Description                 │
├──────────────────┼───────────────┼─────────────────────────────┤
│ date             │ DATE          │ Trading date                │
│ symbol           │ STRING        │ Stock ticker                │
│ vwap             │ FLOAT         │ Volume weighted avg price   │
│ total_volume     │ LONG          │ Total shares traded         │
│ trade_count      │ INTEGER       │ Number of price events      │
│ day_low          │ FLOAT         │ Lowest price of the day     │
│ day_high         │ FLOAT         │ Highest price of the day    │
└──────────────────┴───────────────┴─────────────────────────────┘
```

### Moving Averages Table — `moving_avg`
```
┌──────────────────┬───────────────┬─────────────────────────────┐
│ Column           │ Type          │ Description                 │
├──────────────────┼───────────────┼─────────────────────────────┤
│ symbol           │ STRING        │ Stock ticker                │
│ price            │ FLOAT         │ Current price               │
│ prev_price       │ FLOAT         │ Previous event price        │
│ price_change     │ FLOAT         │ Change from previous        │
│ ma_3             │ FLOAT         │ 3-event moving average      │
│ ma_7             │ FLOAT         │ 7-event moving average      │
│ timestamp        │ TIMESTAMP     │ Event time                  │
└──────────────────┴───────────────┴─────────────────────────────┘
```

### Price Spikes Table — `price_spikes`
```
┌──────────────────┬───────────────┬─────────────────────────────┐
│ Column           │ Type          │ Description                 │
├──────────────────┼───────────────┼─────────────────────────────┤
│ symbol           │ STRING        │ Stock ticker                │
│ price            │ FLOAT         │ Event price                 │
│ rolling_avg      │ FLOAT         │ 5-event rolling average     │
│ deviation_pct    │ FLOAT         │ % deviation from rolling avg│
│ is_spike         │ STRING        │ YES / NO                    │
│ spike_severity   │ STRING        │ HIGH / MEDIUM / LOW / NORMAL│
│ timestamp        │ TIMESTAMP     │ Event time                  │
└──────────────────┴───────────────┴─────────────────────────────┘
```

<br/>

---

## 📈 Financial Metrics

### VWAP (Volume Weighted Average Price)

The most important intraday price metric used by institutional traders.

```
VWAP = Σ(Price × Volume) / Σ(Volume)

Example:
  10:00 AM → $248.00 × 1,000 shares
  10:05 AM → $250.00 × 5,000 shares  ← heavy trading
  10:10 AM → $246.00 × 500  shares

  Simple Avg = $248.00
  VWAP       = $249.54  ← more accurate, weighted by volume
```

**Trading signal:** Price above VWAP = expensive. Price below VWAP = cheap.

---

### Moving Averages (MA3 & MA7)

Smooths price noise to reveal underlying trend direction.

```
MA3 (3-event) → reacts quickly → short-term momentum
MA7 (7-event) → reacts slowly  → long-term trend

Golden Cross: MA3 crosses above MA7 → BUY signal  📈
Death Cross:  MA3 crosses below MA7 → SELL signal 📉
```

---

### Price Spike Detection

Identifies statistically unusual price movements in real time.

```
Rolling Average (last 5 events) = $367.96

Deviation % = |Current Price - Rolling Avg| / Rolling Avg × 100

Severity thresholds:
  > 3% deviation → HIGH   🚨  (major event — earnings, news)
  > 2% deviation → MEDIUM ⚠️  (significant movement)
  > 1% deviation → LOW    📊  (minor fluctuation)
  < 1% deviation → NORMAL ✅
```

<br/>

---

## 🏆 Delta Lake Best Practices

### Partitioning Strategy

```
stock_prices_partitioned/
├── date=2026-03-23/
│   ├── symbol=AAPL/  → part-0001.parquet
│   ├── symbol=AMZN/  → part-0001.parquet
│   ├── symbol=GOOGL/ → part-0001.parquet
│   ├── symbol=MSFT/  → part-0001.parquet
│   └── symbol=TSLA/  → part-0001.parquet
└── date=2026-03-26/
    └── (same structure)
```

**Impact:** A query for `AAPL on 2026-03-26` scans **10 rows** instead of all 98. At production scale (billions of rows), this is the difference between a 2-second query and a 20-minute one.

### File Compaction (OPTIMIZE)

Streaming jobs create many small files — one per micro-batch. OPTIMIZE merges them:

```
Before OPTIMIZE: 146 small files (~20 bytes each)
After OPTIMIZE:  1 large file  (3,030 bytes total)

Query performance improvement: ~10-50x at scale
```

### Audit Trail (Delta History)

Every operation logged — 146 versions captured:

```
Version 146 → VACUUM END       (maintenance)
Version 145 → VACUUM START     (maintenance)
Version 144 → OPTIMIZE         (compaction)
Version 143 → STREAMING UPDATE (06:24 AM batch)
Version 142 → STREAMING UPDATE (06:23 AM batch)
...
Version 0   → Initial write
```

<br/>

---

## 🔄 Orchestration

### Airflow DAG — `stock_pipeline_daily`

```
Schedule: 0 6 * * *  (6:00 AM every day)

task_vwap ──────────────────────┐
                                │
task_moving_avg ────────────────┼──▶ task_optimize ──▶ task_quality ──▶ task_summary
                                │
task_spike_detection ───────────┘

Tasks 1-3 run in PARALLEL → Task 4 runs after all complete
```

| Task | Type | Purpose |
|---|---|---|
| `vwap_calculation` | DatabricksNotebook | Compute daily VWAP |
| `moving_averages` | DatabricksNotebook | Compute MA3 & MA7 |
| `spike_detection` | DatabricksNotebook | Flag anomalous prices |
| `optimize_vacuum` | DatabricksNotebook | Compact + clean Delta tables |
| `data_quality_check` | PythonOperator | Validate row counts |
| `pipeline_summary` | PythonOperator | Log completion metrics |

**Retry policy:** 2 retries with 5-minute delay on any task failure.

<br/>

---

## 📊 Results & Insights

### Pipeline Statistics

```
Raw events ingested     : 98 events across 2 trading days
Symbols tracked         : 5 (AAPL, MSFT, GOOGL, AMZN, TSLA)
Aggregated windows      : 35 (5-minute tumbling windows)
Spikes detected         : 14 out of 98 events (14.3%)
Delta Lake versions     : 146 (complete audit trail)
Streaming micro-batches : 143 (every 30 seconds)
```

### VWAP Results (2 Trading Days)

| Symbol | Mar 23 VWAP | Mar 26 VWAP | Change |
|---|---|---|---|
| AAPL | $247.99 | $252.62 | +1.9% 📈 |
| AMZN | $205.37 | $211.71 | +3.1% 📈 |
| GOOGL | $301.00 | $290.93 | -3.3% 📉 |
| MSFT | $381.87 | $371.04 | -2.8% 📉 |
| TSLA | $367.96 | $385.95 | +4.9% 📈 |

### Moving Average Signals

| Symbol | MA3 | MA7 | Signal |
|---|---|---|---|
| AAPL | 249.80 | 249.40 | MA3 > MA7 → Bullish 📈 |
| TSLA | 376.48 | 374.59 | MA3 > MA7 → Bullish 📈 |
| AMZN | 208.35 | 207.61 | MA3 > MA7 → Bullish 📈 |
| GOOGL | 296.47 | 297.48 | MA3 < MA7 → Bearish 📉 |
| MSFT | 377.31 | 378.45 | MA3 < MA7 → Bearish 📉 |

### Spike Detection Summary

| Symbol | HIGH | MEDIUM | LOW | NORMAL |
|---|---|---|---|---|
| AAPL | 0 | 0 | 2 | 21 |
| AMZN | 0 | 1 | 2 | 14 |
| GOOGL | 0 | 2 | 1 | 17 |
| MSFT | 0 | 1 | 2 | 16 |
| TSLA | 1 🚨 | 1 | 1 | 16 |

**TSLA was the most volatile** — matching real-world market behavior.

<br/>

---

## 🚀 Getting Started

### Prerequisites

```
✅ Azure account (free tier — $200 credits)
✅ Python 3.10+
✅ Docker Desktop
✅ Power BI Desktop
✅ Git
```

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/azure-stock-pipeline.git
cd azure-stock-pipeline
```

### 2. Setup Python Environment

```bash
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
ALPHA_VANTAGE_API_KEY=your_api_key
EVENT_HUBS_CONNECTION_STRING=your_connection_string;EntityPath=stock-prices
EVENT_HUB_NAME=stock-prices
STORAGE_ACCOUNT_NAME=stockpipelinelake
STORAGE_ACCOUNT_KEY=your_storage_key
DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN=your_databricks_token
```

### 4. Provision Azure Resources

```
Azure Portal:
  1. Create Resource Group    : stock-pipeline-rg
  2. Create Event Hubs        : stock-pipeline-eh (Basic tier)
     └── Create Event Hub     : stock-prices (2 partitions)
  3. Create Storage Account   : stockpipelinelake (ADLS Gen2)
     └── Create Containers    : raw, aggregated, checkpoints
  4. Create Databricks         : stock-pipeline-dbx (Trial tier)
     └── Create Cluster       : stock-cluster (13.3 LTS, DS3_v2)
     └── Install Library      : com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
```

### 5. Start the Producer

```bash
python ingestion/producer.py
```

Expected output:
```
🚀 Starting stock price producer...
📈 AAPL: $252.62 | 2026-03-26T06:00:01
📈 MSFT: $371.04 | 2026-03-26T06:00:02
📈 GOOGL: $290.93 | 2026-03-26T06:00:03
📈 AMZN: $211.71 | 2026-03-26T06:00:04
📈 TSLA: $385.95 | 2026-03-26T06:00:05
✅ Sent 5 events to Event Hubs
⏳ Waiting 60 seconds...
```

### 6. Run Databricks Notebooks

In your Databricks workspace, run in order:
```
1. mount_storage.py         → configure ADLS access
2. streaming_job.py         → start real-time ingestion
3. batch_jobs.py            → run VWAP, MA, spike detection
4. delta_best_practices.py  → optimize & verify tables
```

### 7. Start Airflow

```bash
cd airflow
docker-compose up airflow-init
docker-compose up -d
```

Open **http://localhost:8080** → Login: `admin` / `admin`

Add Databricks connection:
```
Admin → Connections → + Add
  Connection Id   : databricks_default
  Connection Type : Databricks
  Host            : https://adb-xxxx.azuredatabricks.net
  Password        : your_databricks_token
```

Enable DAG: `stock_pipeline_daily` ✅

<br/>

---

## 🧠 Architecture Decisions

### Why Lambda Architecture instead of pure streaming?

PySpark Structured Streaming with watermarks delays aggregated results until the watermark period elapses — in testing, a 10-minute watermark meant waiting 15+ minutes for results. Separating raw ingestion (streaming) from analytics (batch) gives sub-30-second raw data latency while keeping batch jobs predictable and cost-efficient.

### Why Delta Lake instead of plain Parquet?

Streaming jobs produce hundreds of small Parquet files. Without Delta Lake, reading them requires listing all files on every query. Delta Lake's transaction log makes file discovery O(1) regardless of file count. ACID transactions also prevent partial reads during concurrent batch writes.

### Why Airflow instead of Azure Data Factory?

Airflow DAGs are Python code — version-controlled, testable, and cloud-agnostic. ADF pipelines are JSON configs that live inside Azure. For a portfolio project, Python-first tooling demonstrates deeper engineering skill and transfers to any stack.

### Why partition by both date AND symbol?

Most analytical queries filter on at least one of these dimensions. Partitioning by date alone still scans all symbols. Partitioning by both lets Spark skip entire folders — a `date=2026-03-26/symbol=AAPL` query scans exactly 1 partition file out of 10.

### Why `overwrite` mode on batch jobs?

Batch jobs are idempotent — re-running produces identical results. Using `overwrite` means a failed mid-run job leaves no partial state to clean up. On retry, Airflow re-runs the full job cleanly.

<br/>

---

## 💰 Cost Analysis

| Resource | Tier | Estimated Cost |
|---|---|---|
| Azure Databricks | Trial (14 days free) | ₹150-300/hr while running |
| Azure Event Hubs | Basic | ~₹600/month |
| Azure Data Lake Gen2 | Standard | ~₹2/GB/month |
| Apache Airflow | Self-hosted (Docker) | Free |
| Power BI Desktop | Free tier | Free |
| GitHub Actions | Free tier | Free |
| **Total project cost** | | **~₹3,500-4,500** |

**Available Azure free credit:** ₹16,000 ($200)

**Cost saving measures applied:**
- Auto-terminate cluster after 30 minutes inactivity
- Single-node cluster (Standard_DS3_v2)
- Basic Event Hubs tier (1 throughput unit)
- VACUUM removes old Delta files monthly
- Partitioning reduces query scan costs

<br/>

---

## 📚 What I Learned

### Technical

- **PySpark Structured Streaming** — readStream, writeStream, watermarks, triggers, checkpointing
- **Delta Lake internals** — transaction log, file compaction, time travel, partition pruning
- **Azure Event Hubs** — throughput units, consumer groups, EntityPath configuration
- **Lambda Architecture** — when to use streaming vs batch and why
- **Airflow DAGs** — task dependencies, parallel execution, retry policies, PythonOperator vs DatabricksOperator
- **ELT pattern** — loading raw data first, transforming inside the platform

### Financial Domain

- **VWAP** — why volume-weighted average is more accurate than simple average
- **Moving averages** — how MA crossovers generate buy/sell signals
- **Price spike detection** — how trading systems use statistical deviation for risk monitoring
- **Market microstructure** — why the same stock has different VWAP on different days

### Engineering Judgment

- Watermark delays in streaming aggregations — and when to switch to batch
- File proliferation in streaming pipelines — and how OPTIMIZE solves it
- Cost management on cloud platforms — auto-termination, right-sizing, budget alerts
- Why idempotent batch jobs matter for production reliability

<br/>

---

## 🗺️ Roadmap

- [ ] Add **Great Expectations** for automated data quality checks
- [ ] Add **Azure Monitor** alerts for pipeline failures
- [ ] Implement **Medallion Architecture** (Bronze → Silver → Gold layers)
- [ ] Add **more stock symbols** (S&P 500 universe)
- [ ] Add **options data** (IV, Greeks) for derivatives analytics
- [ ] Replace Alpha Vantage with **Polygon.io** for higher frequency data
- [ ] Add **ML anomaly detection** using Databricks MLflow
- [ ] Deploy Airflow to **Azure Container Instances** (remove local Docker dependency)
- [ ] Add **Terraform** for full infrastructure-as-code

<br/>

---

## 👤 Author

**Your Name**
Data Engineer | 2.5 years experience | Azure · Databricks · PySpark

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://linkedin.com/in/yourprofile)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat&logo=github)](https://github.com/yourusername)

---

*Built as a portfolio project to demonstrate production-grade data engineering skills on Azure.*

---

## 📄 License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.
