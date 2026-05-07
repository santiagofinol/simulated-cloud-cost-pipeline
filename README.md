# ☁️ Simulated Cloud Cost Monitoring & Anomaly Detection Pipeline

> An end-to-end data pipeline that ingests cloud billing metrics every 30 minutes,
> Simulated forecasts expected costs with Prophet, flags anomalies with Isolation Forest,
> and visualizes everything in a Streamlit dashboard.


[![Pipeline](https://github.com/santiagofinol/simulated-cloud-cost-pipeline/actions/workflows/pipeline.yml/badge.svg)](https://github.com/santiagofinol/simulated-cloud-cost-pipeline/actions/workflows/pipeline.yml)

---

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────────┐
│  generate_data  │───▶│  data/raw/   │───▶│    transform.py     │
│  (synthetic API)│    │  *.json      │    │  (star schema load) │
└─────────────────┘    └──────────────┘    └──────────┬──────────┘
         ▲                                             │
         │                                             ▼
  GitHub Actions                           ┌─────────────────────┐
  (cron every 30min)                       │   Supabase (PG)     │
         │                                 │   ─────────────     │
         └─────────────────────────────────│   fact_cost         │
                                           │   fact_forecast     │
┌─────────────────┐                        │   fact_anomaly      │
│    train.py     │──────────────────────▶ │   dim_*             │
│  (Prophet)      │                        └──────────┬──────────┘
└─────────────────┘                                   │
┌─────────────────┐                                   │
│    score.py     │◀──────────────────────────────────┘
│ (Prophet + IsoF)│──────────────────────▶ fact_forecast
└─────────────────┘                        fact_anomaly
                                                       │
                                                       ▼
                                          ┌─────────────────────┐
                                          │  Streamlit Dashboard│
                                          │   Real-time KPIs,   │
                                          │   Charts, Alerts    │
                                          └─────────────────────┘
```

### Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Scheduling | GitHub Actions (cron) | Free, version-controlled, no infra |
| Storage | Supabase (PostgreSQL) | Free tier, cloud-hosted, connection pooling |
| Transformation | Python + pandas | Simple, testable |
| Forecasting | Prophet | Daily/weekly seasonality, minimal tuning |
| Anomaly Detection | Isolation Forest | Unsupervised, no labelled data needed |
| Visualization | Streamlit | real-time, responsive |

---

## Star Schema ERD

```
              ┌────────────┐
              │  dim_date  │
              │────────────│
              │ date_key PK│◀──────────────┐
              │ full_date  │               │
              │ year/month │               │
              │ quarter    │               │
              └────────────┘               │
                                           │
┌─────────────┐    ┌──────────────────┐    │
│ dim_service │    │    fact_cost     │    │
│─────────────│    │──────────────────│    │
│ service_key │◀──▶│ cost_id       PK │    │
│ service_name│    │ date_key      FK │────┘
│ service_type│    │ time_key      FK │────┐
└─────────────┘    │ service_key   FK │    │
                   │ region_key    FK │    │
┌─────────────┐    │ cost_usd         │    │
│ dim_region  │    │ resource_count   │    │
│─────────────│    │ ingested_at      │    │
│ region_key  │◀──▶└──────────────────┘    │
│ region_code │             │              │
│ geography   │             │              │    ┌───────────┐
└─────────────┘    ┌────────▼───────┐     │    │ dim_time  │
                   │ fact_anomaly   │     └───▶│───────────│
                   │────────────────│          │ time_key  │
                   │ anomaly_id  PK │          │ hour      │
                   │ cost_id     FK │          │ minute    │
                   │ is_anomaly     │          │time_of_day│
                   │ severity       │          └───────────┘
                   │ anomaly_score  │
                   └────────────────┘

   ┌──────────────────────────────────┐
   │         fact_forecast            │
   │──────────────────────────────────│
   │ forecast_id PK                   │
   │ service_key FK → dim_service     │
   │ region_key  FK → dim_region      │
   │ forecast_ts, yhat, lower, upper  │
   └──────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Python 3.11+
- Free [Supabase](https://supabase.com) account
- Git

### 1. Clone & install

```bash
git clone https://github.com/santiagofinol/cloud-cost-pipeline.git
cd cloud-cost-pipeline
python -m venv .venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and set DB_URL to your specific Supabase connection string
```

Get your Supabase connection string:
`Supabase Dashboard → Project → Settings → Database → Connection string → URI`
I used **Transaction mode (port 6543)** for connection pooling.

### 3. Create database schema

Copy `sql/schema.sql` → `sql/seed.sql` into Supabase SQL Editor and run both.

### 4. Backfill 60 (or more if you want, would have better results) days of historical data

```bash
python scripts/generate_data.py backfill 60
python scripts/transform.py
```

### 5. Train Prophet models

```bash
python scripts/train.py
# Output: "Trained 15 Prophet models"
```

### 6. Run scoring pipeline

```bash
python scripts/score.py
```

### 7. Launch Streamlit dashboard

```bash
streamlit run scripts/dashboard.py
```

Dashboard opens with live KPIs, charts, and anomaly tables.

### 8. Set up GitHub Actions

1. Push to GitHub
2. **Settings → Secrets and variables → Actions → New repository secret**
3. Add: `DB_URL` = your Supabase connection string
4. Pipeline runs automatically every 30 minutes (or manually via Actions tab)

---

## Dashboard Features

**Real-time KPIs:**
- Current hour cost (with DoD % change)
- Anomalies today (with severity breakdown)
- MTD variance % (actual vs forecast)
- Period total cost (with WoW % change)

**Variance Details Expander:**
- MTD actual vs forecast cost breakdown
- Yesterday's same-hour cost (DoD diagnostic)
- Explanations for high variance % (useful for interview discussions)

**Visualizations:**
- Time-series: Actual costs by service
- Forecast with 90% confidence bands
- Service cost breakdown (bar chart)
- Region distribution (donut chart)
- Anomaly severity distribution

**Data Explorer:**
- Raw cost data table (expandable)
- Forecast data table (expandable)
- Anomaly records with timestamps

**Filters (sidebar):**
- Multi-select by service and region
- Date range picker
- Auto-refresh every 5 minutes

---

## Scripts Reference

| Script | Run Frequency | Purpose |
|---|---|---|
| `scripts/generate_data.py` | Every 30 min (CI) | Simulates cloud API, writes JSON |
| `scripts/transform.py` | Every 30 min (CI) | ETL: JSON → star schema |
| `scripts/train.py` | Weekly / manually | Trains Prophet models |
| `scripts/score.py` | Every 30 min (CI) | Forecasts + anomaly detection |
| `scripts/dashboard.py` | On-demand | Streamlit dashboard |

### Backfill mode

```bash
python scripts/generate_data.py backfill 90
```

---

## Running Tests

```bash
pip install pytest pytest-cov
pytest tests/ -v --cov=scripts
```

---

## Project Structure

```
cloud-cost-pipeline/
├── .github/
│   └── workflows/
│       ├── pipeline.yml     # 30-min cron: generate → transform → score
│       └── ci.yml           # PR/push unit tests
├── data/raw/                # gitignored — transient JSON
├── models/                  # gitignored — trained via GH Artifacts
├── scripts/
│   ├── generate_data.py     # Synthetic billing simulator
│   ├── transform.py         # Star schema ETL
│   ├── train.py             # Prophet training
│   ├── score.py             # Forecasting + anomaly detection
│   └── dashboard.py         # Streamlit app
├── sql/
│   ├── schema.sql           # DDL: all tables
│   └── seed.sql             # Dimension seed data
├── tests/
│   ├── test_generate.py
│   ├── test_transform.py
│   └── test_score.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```


