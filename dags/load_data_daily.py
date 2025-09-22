from __future__ import annotations

import pendulum
import json
import os

from airflow.sdk import dag,task,Context
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


@dag(
    start_date=pendulum.datetime(2025, 9, 18, tz="Asia/Taipei"),
    schedule="0 5 * * *",
    catchup=False,
    tags=["dbt", "data_ingestion"],
)
def load_data_daily_to_bigquery():
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
    
      # --- 天氣資料的處理，使用動態任務映射 ---

    # 2.1: 列出所有需要清理的檔案
    @task
    def list_weather_files(**kwargs):
        gcs_hook = GCSHook(gcp_conn_id="gcp_bucket")
        # 直接使用傳入的 execution_date 參數來構建路徑
        execution_date = kwargs['data_interval_start']
        yesterday_date_path = execution_date.subtract(days=1).strftime("%Y/%m/%d")
        source_path = f"weather_raw/{yesterday_date_path}/"
        
        # 返回一個檔案路徑列表
        return gcs_hook.list(bucket_name=gcs_bucket, prefix=source_path)

    # 2.2: 清理單個檔案
    @task
    def clean_single_file(raw_file: str):
        gcs_hook = GCSHook(gcp_conn_id="gcp_bucket")
        cleaned_path = raw_file.replace('weather_raw/', 'weather_cleaned/')
        
        raw_data = gcs_hook.download(bucket_name=gcs_bucket, object_name=raw_file).decode('utf-8')
        
        try:
            full_json = json.loads(raw_data)
            stations = full_json.get('cwaopendata', {}).get('dataset', {}).get('Station', [])
            cleaned_lines = [json.dumps(station) for station in stations]
            
            if cleaned_lines:
                cleaned_data = '\n'.join(cleaned_lines)
                gcs_hook.upload(bucket_name=gcs_bucket, object_name=cleaned_path, data=cleaned_data.encode('utf-8'))
        
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error processing file {raw_file}: {e}")
            raise

    # 2.3: 載入清理後的資料
    load_cleaned_weather_data = GCSToBigQueryOperator(
        task_id="load_weather_data",
        gcp_conn_id="gcp_bucket",
        bucket=gcs_bucket,
        source_objects=[
            'weather_cleaned/{{ data_interval_start.subtract(days=1).format("YYYY") }}/'
            '{{ data_interval_start.subtract(days=1).format("MM") }}/'
            '{{ data_interval_start.subtract(days=1).format("DD") }}/*.json'
        ],
        destination_project_dataset_table=f"{staging_dataset}.raw_weather_data",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    list_files_task = list_weather_files()
    clean_files_task = clean_single_file.partial().expand(raw_file=list_files_task)
    clean_files_task >> load_cleaned_weather_data

    load_ubike_data


load_data_daily_to_bigquery()
