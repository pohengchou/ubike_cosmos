from __future__ import annotations

import pendulum
import json
import os
from datetime import datetime
from airflow.sdk import dag, task, Context
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# DBT 配置常數
DBT_PROJECT_PATH = "/usr/local/airflow/dbt/dbt_demo"
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/ubike_cosmos/bin/dbt"
BIGQUERY_DATASET = "ubike_471005_data_warehouse"
KEYFILE_PATH = "/usr/local/airflow/include/keys/service_account.json"

# Cosmos 配置
_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH
)

_profile_config = ProfileConfig(
    profile_name="dbt_demo",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="Bigquery",
        profile_args={
            "project": "ubike-471005",
            "dataset": "ubike_471005_data_warehouse", 
            "keyfile": KEYFILE_PATH
        }
    )
)

_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

@dag(
    start_date=pendulum.datetime(2025, 9, 18, tz="Asia/Taipei"),
    schedule="0 5 * * *",  # 修正 cron 表達式
    catchup=False,
    tags=["dbt", "data_ingestion", "data_warehouse"],
    max_active_tasks=4,
)
def integrated_data_pipeline():
    """
    整合的資料管道 DAG：
    1. 每日從 GCS 載入 Ubike 和天氣資料到 BigQuery staging
    2. 執行 dbt 轉換建立資料倉儲（顯示詳細的 model 執行狀態）
    """
    
    # 從 Airflow Variables 讀取設定
    gcs_bucket = Variable.get("GCS_BUCKET_NAME")
    staging_dataset = Variable.get("BIGQUERY_STAGING_DATASET")
    
    # === 第一階段：資料載入 ===
    
    # 1. 載入 Ubike 資料
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
    
    # 2. 天氣資料處理流程
    @task
    def list_weather_files(**kwargs):
        """列出需要處理的天氣資料檔案"""
        gcs_hook = GCSHook(gcp_conn_id="gcp_bucket")
        execution_date = kwargs['data_interval_start']
        yesterday_date_path = execution_date.subtract(days=1).strftime("%Y/%m/%d")
        source_path = f"weather_raw/{yesterday_date_path}/"
        
        return gcs_hook.list(bucket_name=gcs_bucket, prefix=source_path)
    
    @task
    def clean_single_file(raw_file: str):
        """清理單個天氣資料檔案"""
        gcs_hook = GCSHook(gcp_conn_id="gcp_bucket")
        cleaned_path = raw_file.replace('weather_raw/', 'weather_cleaned/')
        
        raw_data = gcs_hook.download(bucket_name=gcs_bucket, object_name=raw_file).decode('utf-8')
        
        try:
            full_json = json.loads(raw_data)
            stations = full_json.get('cwaopendata', {}).get('dataset', {}).get('Station', [])
            cleaned_lines = [json.dumps(station) for station in stations]
            
            if cleaned_lines:
                cleaned_data = '\n'.join(cleaned_lines)
                gcs_hook.upload(
                    bucket_name=gcs_bucket, 
                    object_name=cleaned_path, 
                    data=cleaned_data.encode('utf-8')
                )
                return cleaned_path
        
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error processing file {raw_file}: {e}")
            raise
    
    # 3. 載入清理後的天氣資料
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
    
    # === 第二階段：dbt 轉換（顯示詳細 lineage） ===
    
    # 4. 使用 DbtTaskGroup 顯示所有 dbt models 的詳細執行狀態
    dbt_models = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        operator_args={
            "install_deps": True,
            "full_refresh": False,
        },
    )
    
    # === 任務依賴設定 ===
    
    # 天氣資料處理流程
    list_files_task = list_weather_files()
    clean_files_task = clean_single_file.partial().expand(raw_file=list_files_task)
    clean_files_task >> load_cleaned_weather_data
    
    # 主要資料流程依賴
    # 兩個載入任務完成後，才執行 dbt 轉換群組
    [load_ubike_data, load_cleaned_weather_data] >> dbt_models

# 實例化 DAG
integrated_data_pipeline()