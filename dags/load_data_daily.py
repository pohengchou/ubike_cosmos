from __future__ import annotations

import pendulum

from airflow.sdk import dag
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


@dag(
    start_date=pendulum.datetime(2025, 9, 18, tz="Asia/Taipei"),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "data_ingestion"],
)
def load_data_daily():
    """
    這個 DAG 每日自動從 GCS 載入 Ubike 和天氣資料到 BigQuery staging 資料表。
    """

    # 從 Airflow Variables 讀取設定
    gcs_bucket = Variable.get("GCS_BUCKET_NAME")
    staging_dataset = Variable.get("BIGQUERY_STAGING_DATASET")

    load_ubike_data = GCSToBigQueryOperator(
        task_id="load_ubike_data",
        gcp_conn_id="gcp_bucket", 
        bucket=gcs_bucket,
        source_objects=[
            'ubike_raw/{{ data_interval_start.subtract(days=1).format("YYYY") }}/'
            '{{ data_interval_start.subtract(days=1).format("MM") }}/'
            '{{ data_interval_start.subtract(days=1).format("DD") }}/*.json'
        ],
        destination_project_dataset_table=f"{staging_dataset}.raw_ubike_data",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    
    load_weather_data = GCSToBigQueryOperator(
        task_id="load_weather_data",
        gcp_conn_id="gcp_bucket",
        bucket=gcs_bucket,
        source_objects=[
            'weather_raw/{{ data_interval_start.subtract(days=1).format("YYYY") }}/'
            '{{ data_interval_start.subtract(days=1).format("MM") }}/'
            '{{ data_interval_start.subtract(days=1).format("DD") }}/*.json'
        ],
        destination_project_dataset_table=f"{staging_dataset}.raw_weather_data",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )


load_data_daily()
