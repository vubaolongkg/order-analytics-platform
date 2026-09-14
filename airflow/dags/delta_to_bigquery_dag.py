import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from deltalake import DeltaTable
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = "velvety-citizen-466214-r4"
DATASET_ID = "raw_staging"
TABLE_NAME = "bronze_orders"
DELTA_TABLE_PATH = "/opt/airflow/delta_lake/bronze_orders"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def sync_delta_to_bigquery():
    if not os.path.exists(DELTA_TABLE_PATH):
        print(f"Chưa tìm thấy thư mục Delta Lake tại {DELTA_TABLE_PATH}, bỏ qua lượt chạy.")
        return

    # 1. Đọc bảng Delta Lake Bronze sang Pandas qua Apache Arrow
    dt = DeltaTable(DELTA_TABLE_PATH)
    df = dt.to_pandas()

    if df.empty:
        print("Bảng Delta Lake hiện chưa có bản ghi nào.")
        return

    print(f"Đọc thành công {len(df)} bản ghi từ Delta Lake Bronze.")

    # 2. Xử lý kiểu dữ liệu trước khi nạp vào BigQuery
    # Cột items là list chuỗi -> chuyển thành json string để BigQuery dễ lưu trữ
    if "items" in df.columns:
        df["items"] = df["items"].apply(lambda x: str(x) if x is not None else None)

    # 3. Nạp dữ liệu vào Google BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Giữ snapshot bản mới nhất từ Delta Lake
        autodetect=True,
    )

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Đợi load hoàn tất

    print(f"Đã đồng bộ {len(df)} dòng vào BigQuery: {table_ref}")

with DAG(
    dag_id="sync_bronze_delta_to_bigquery",
    default_args=default_args,
    description="Batch ETL: Đồng bộ dữ liệu Delta Lake Bronze lên BigQuery Staging",
    schedule_interval=timedelta(minutes=15),
    catchup=False,
) as dag:

    sync_task = PythonOperator(
        task_id="sync_delta_bronze_to_bq",
        python_callable=sync_delta_to_bigquery,
    )

    sync_task