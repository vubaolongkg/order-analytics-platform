# Real-Time Food Delivery Order Analytics Platform

A data engineering project that ingests, processes, stores, transforms, and visualizes food delivery order events in near real time. The stack runs Redpanda (Kafka-compatible), PySpark Structured Streaming, Delta Lake, Apache Airflow, Google BigQuery, dbt, and Google Looker Studio.

I built this to work through a full streaming-to-warehouse pipeline end to end, not just the streaming part or just the modeling part in isolation.

---

## Architecture

```text
Python Producer (order lifecycle events)
        |
        v
Redpanda / Kafka broker (topic: delivery_orders)
        |
        v
PySpark Structured Streaming (sliding-window aggregation)
        |
        v
Local Delta Lake storage
  - Bronze layer: raw, append-only
  - Silver layer: cleaned, stateful aggregates
        |
        v
Apache Airflow (scheduled ingestion pipeline)
        |
        v
Google BigQuery (raw_staging.bronze_orders)
        |
        v
dbt-bigquery core
  - Staging: stg_orders view with deduplication
  - Marts (star schema): dim_restaurants, fct_orders, fct_hourly_metrics
        |
        v
Google Looker Studio (operations dashboard + geospatial view)
```

## Tech stack and why each piece is there

| Component | Technology | Why it's there |
|---|---|---|
| Event streaming | Redpanda (Kafka API) | Kafka-compatible without the JVM overhead, easy to run locally for order lifecycle pub/sub. |
| Stream processing | PySpark Structured Streaming | Micro-batch processing with watermarking for sliding-window metrics like GMV and order frequency. |
| Storage | Delta Lake (Bronze/Silver) | ACID guarantees, time travel, and Parquet under the hood on local storage. |
| Orchestration | Apache Airflow 2.8+ | Schedules the Bronze-to-warehouse load and kicks off dbt afterward. |
| Warehouse | Google BigQuery | Serverless analytics warehouse, running on the sandbox tier here. |
| Transformation | dbt | Medallion modeling, SQL compilation, dedup logic, and data tests. |
| BI | Google Looker Studio | Geospatial heatmaps over Ho Chi Minh City, KPI scorecards, hourly trends. |
| Containers | Docker / Docker Compose | Runs Redpanda, Spark, Airflow, and Postgres together. |

## Data modeling

The transformation layer follows a fairly standard medallion setup in dbt:

**Bronze** (`raw_staging.bronze_orders`) — raw order events loaded straight from Delta Lake by an Airflow batch job, using `WRITE_TRUNCATE`.

**Silver / staging** (`raw_staging_staging.stg_orders`) — a view that deduplicates events with `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp DESC)` to keep only the latest state per order, plus type casting and timestamp cleanup.

**Gold / marts** (`raw_staging_marts`):
- `dim_restaurants` — restaurant entities, cuisine type, GPS coordinates
- `fct_orders` — one row per order, with delivery state (PENDING, PREPARING, IN_TRANSIT, CANCELLED) and order value
- `fct_hourly_metrics` — hourly rollups of volume, net GMV, and cancellation rate

## Data quality

dbt tests run on every build:
- Uniqueness on primary keys (`dim_restaurants.restaurant_id`, `fct_orders.order_id`)
- Not-null checks on key business columns (`total_amount`, `metric_hour`, `total_orders`)
- All tests currently pass across dimensions and facts.

## Project structure

```text
order-analytics-platform/
├── airflow/
│   ├── dags/
│   │   └── delta_to_bigquery_dag.py     # batch loader + dbt trigger
│   ├── Dockerfile                       # Airflow image with BigQuery/Delta deps
│   └── keys/                            # service account credentials (gitignored)
├── dbt_transforms/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── schema.yml               # source definitions
│   │   │   └── stg_orders.sql           # staging view + dedup
│   │   └── marts/
│   │       ├── schema.yml               # tests/constraints
│   │       ├── dim_restaurants.sql
│   │       ├── fct_orders.sql
│   │       └── fct_hourly_metrics.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── ingestion/
│   ├── producer.py                      # simulates HCMC order events
│   └── requirements.txt
├── streaming/
│   ├── spark_streaming.py               # structured streaming job
│   └── delta_lake/                      # local Bronze/Silver tables
├── docker-compose.yml
└── README.md
```

## Getting started

### Prerequisites
- Docker and Docker Compose
- Python 3.9+
- A GCP project with BigQuery enabled, and a service account key with BigQuery Admin

### 1. Configure environment
Drop your service account key at:
```
airflow/keys/gcp_key.json
```
Create the `raw_staging` dataset in BigQuery (location: US).

### 2. Start the infrastructure
```powershell
docker compose up -d --build
```
This brings up Redpanda, Spark Streaming, the Airflow webserver and scheduler, and Postgres.

### 3. Start ingestion and processing
In a new terminal:
```powershell
python ingestion/producer.py
```
Watch the streaming job:
```powershell
docker logs -f spark-streaming
```

### 4. Run the pipeline
Open the Airflow UI at `http://localhost:8085` (login: admin / admin), unpause `sync_bronze_delta_to_bigquery`, and trigger it. It pulls the Bronze snapshot from Delta Lake, loads it into BigQuery, then runs `dbt run` followed by `dbt test`.

### 5. Run dbt manually (optional)
```powershell
cd dbt_transforms
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Dashboard

The Looker Studio dashboard connects live to BigQuery and shows:
- Operational KPIs: net GMV, order count, cancellation rate
- Hourly order and revenue trends by restaurant category
- A geospatial bubble map over Ho Chi Minh City districts (1, 3, 5) using order coordinates