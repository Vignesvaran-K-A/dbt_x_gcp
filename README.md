# 🏙️ NYC 311 End-to-End ELT Data Pipeline & Analytics Platform

An enterprise-grade, cloud-native data engineering pipeline that ingests, cleans, transforms, and visualizes real-time NYC 311 public service request data. Built with modern data stack principles using the **Medallion Architecture**, automated CI/CD workflows, modular dbt transformations, and an interactive analytics frontend.

---

## 🏗️ Architecture & Tech Stack

```text
┌─────────────────────────┐
│   NYC 311 Socrata API   │
└────────────┬────────────┘
             │  Python Ingestion (Dynamic Date Filtering & Pagination)
             ▼
┌────────────────────────────────────────────────────────┐
│  Google Cloud BigQuery: Bronze Layer (raw_nyc_311)     │
└────────────┬───────────────────────────────────────────┘
             │  dbt (Deduplication, Type Casting, Data Tests)
             ▼
┌────────────────────────────────────────────────────────┐
│  Google Cloud BigQuery: Silver Layer                   │
│  ├─ stg_nyc_311             (Cleaned & Standardized)   │
│  └─ mart_agency_performance (Aggregated Analytics)     │
└────────────┬───────────────────────────────────────────┘
             │  BigQuery Connector (Parameterized Queries & Caching)
             ▼
┌─────────────────────────┐
│   Streamlit Dashboard   │
└─────────────────────────┘
```

* **Cloud Data Warehouse:** Google Cloud BigQuery
* **Data Transformation:** dbt (`dbt-bigquery`)
* **Ingestion & Automation:** Python, Docker, GitHub Actions CI/CD
* **Frontend Visualization:** Streamlit & Plotly Express

---

## 📊 Key Features & Pipeline Layers

1. **Bronze Layer (Ingestion):**
   * Python-based extractor (`extract_load.py`) that queries NYC Open Data (Socrata API).
   * Supports dynamic date filtering (`--start-date`, `--end-date`), rolling windows (`--days`), and configurable batch limits.
   * Automatic pagination (25,000 records/page) prevents API truncation for multi-month data pulls.
   * Streams raw JSON records directly into BigQuery bronze tables with auto-schema detection.

2. **Silver Layer (Transformations & Modeling):**
   * **`stg_nyc_311`**: Cleans whitespace, converts empty strings to `NULL`, standardizes city casing, validates 5-digit ZIP codes, safely casts timestamps, and removes duplicate requests via window functions (`row_number() over (partition by unique_key order by created_date desc)`).
   * **`mart_agency_performance`**: Aggregates complaint volume, calculates closed case counts, and evaluates agency-level average resolution times in hours.
   * **Data Quality Tests**: Ensures primary key uniqueness and non-null constraints on timestamps (`schema.yml`).

3. **CI/CD Automation (GitHub Actions):**
   * Containerized pipeline packaged with **Docker**.
   * Automated GitHub Actions workflow (`pipeline.yml`) with manual trigger (`workflow_dispatch`).
   * Allows engineers to specify custom start/end dates directly from GitHub's UI to backfill or fetch any historical date range.
   * Automatically executes ingestion, connection verification, and `dbt run` with summary reporting.

4. **Interactive Dashboard (`app.py`):**
   * **High-Level KPIs**: Real-time tracking of total requests, closed cases, active cases, average resolution hours/days, and closure rates.
   * **Daily Volume Trends**: Time-series visualization with a 7-day rolling moving average to identify request spikes.
   * **Top Complaints Breakdown**: Ranked horizontal bar chart highlighting primary municipal complaint categories.
   * **Agency Performance Ranking**: Compares fastest and slowest resolving agencies using pre-aggregated dbt marts.
   * **Dynamic Filters**: Real-time filtering by created date range, municipal agency, and duration units (Hours vs. Days).

---

## 📁 Repository Structure

```text
dbt_x_gcp/
├── .github/workflows/
│   └── pipeline.yml              # GitHub Actions CI/CD ELT workflow
├── nyc_transform/                 # dbt project directory
│   ├── models/
│   │   ├── sources.yml           # Bronze layer BigQuery source definitions
│   │   ├── schema.yml            # Model documentation & data tests
│   │   ├── stg_nyc_311.sql       # Cleaned & deduplicated silver staging model
│   │   └── mart_agency_performance.sql # Aggregated agency performance mart
│   └── dbt_project.yml           # dbt project configuration
├── app.py                        # Streamlit web application & dashboard
├── extract_load.py               # Dynamic ingestion script (Socrata -> BigQuery)
├── Dockerfile                    # Container definition for pipeline execution
├── profiles.yml                  # dbt BigQuery connection profile
├── requirements.txt              # Python project dependencies
└── README.md                     # Project documentation
```

---

## 🚀 Getting Started Locally

### 1. Clone the Repository

```bash
git clone https://github.com/Vignesvaran-K-A/dbt_x_gcp.git
cd dbt_x_gcp
```

### 2. Set Up Environment & Secrets

Create a `secrets/` directory in the root folder and add your GCP Service Account JSON key (must have BigQuery Admin or Data Editor + Job User roles):

```text
secrets/gcp.json
```

Set the Google Cloud credentials environment variable:

* **Windows (PowerShell):**
  ```powershell
  $env:GOOGLE_APPLICATION_CREDENTIALS = "secrets/gcp.json"
  ```
* **Linux / macOS:**
  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS="secrets/gcp.json"
  ```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run Data Ingestion (Extract & Load)

You can run the ingestion script with custom dates, rolling windows, or default to yesterday:

```bash
# Ingest data for a specific date range (e.g. September to October 2026)
python extract_load.py --start-date 2026-09-01 --end-date 2026-10-31 --max-records 50000

# Or ingest the last 30 days
python extract_load.py --days 30

# Or run with defaults (yesterday's data)
python extract_load.py
```

### 5. Run dbt Transformations

Execute the dbt models to update the silver staging views and marts:

```bash
dbt run --project-dir nyc_transform --profiles-dir .
dbt test --project-dir nyc_transform --profiles-dir .
```

### 6. Launch the Streamlit Dashboard

Start the local web dashboard:

```bash
streamlit run app.py
```

The interactive dashboard will open automatically in your browser at `http://localhost:8501`.

---

## ⚙️ Triggering the Pipeline via GitHub Actions

1. Navigate to the **Actions** tab in this GitHub repository.
2. Select **NYC 311 ELT Pipeline** from the workflow sidebar.
3. Click **Run workflow**.
4. (Optional) Provide:
   * **Start Date:** `YYYY-MM-DD` (e.g., `2026-09-01`)
   * **End Date:** `YYYY-MM-DD` (e.g., `2026-10-31`)
   * **Max Records:** e.g., `50000` (or `0` for unlimited)
   * **dbt Target:** `dev` or `prod`
5. Click **Run workflow** to trigger the automated containerized build and execution.

---

## 📈 Dashboard Insights & Visuals

* **Operational Efficiency:** Pinpoint agency bottlenecks by calculating resolution duration per agency and complaint category.
* **Volume Spikes:** Monitor daily incoming request volumes paired with 7-day smoothing to spot seasonality and emergency spikes.
* **Status Tracking:** Real-time visibility into open vs. closed cases to track department backlog.

---

## 📜 License

This project is open-source and available under the [MIT License](LICENSE).

