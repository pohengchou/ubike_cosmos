from __future__ import annotations

import pendulum
import json
import os
from datetime import datetime,timedelta

#airflow相關套件
from airflow.sdk import dag, task, Context
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# cosmos相關套件
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# cosmos模組化
from include.configs.dbt_cosmos_config import(
    PROJECT_CONFIG,
    EXECUTION_CONFIG,
    get_runtime_profile_config
)

# dataproc相關套件
from airflow.providers.google.cloud.operators.dataproc import(
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

#  dataproc模組化
from include.configs.dataproc_config import generate_dataproc_configs

# slack notification套件
from airflow.providers.slack.notifications.slack import SlackNotifier


@dag(
    start_date=pendulum.datetime(2025, 9, 18, tz="Asia/Taipei"),
    schedule="0 5 * * *",  # 修正 cron 表達式
    catchup=False,
    tags=["dbt", "data_ingestion", "data_warehouse"],
    max_active_tasks=4,

    # Slack通知的相關參數
    on_success_callback=SlackNotifier(
        slack_conn_id='slack_default',
        text='integrated_data_pipeline 的 Dag 執行成功!!',
        channel='general',
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack_default',
        text='integrated_data_pipeline 的 Dag 執行失敗',
        channel='general',
    )

)
def integrated_data_pipeline():
    """
    整合的資料管道 DAG：
    1. 每日從 GCS 載入 Ubike 和天氣資料到 BigQuery staging
    2. Dataproc 叢集執行 PySpark 清洗天氣資料（JSON to Parquet）
    3. 執行 dbt 轉換建立資料倉儲
    """
    # ----------------------------------------------------
    # 🎯 模組化配置調用 (I/O 隔離區域)
    # ----------------------------------------------------

    # 1.獲取 Cosmos 運行時 ProfileConfig
    runtime_profile_config = get_runtime_profile_config()
    
    # 2.獲取 Dataproc 所有配置
    # 傳入"{{ ds_nodash }}"給模組
    dataproc_configs = generate_dataproc_configs("{{ ds_nodash }}")
    
    # 解包配置，方便 Task 使用
    CLUSTER_CONFIG = dataproc_configs["cluster_config"]
    SPARK_JOB_CONFIG = dataproc_configs["spark_job_config"]
    CLUSTER_NAME = dataproc_configs["cluster_name"]
    PROJECT_ID = dataproc_configs["project_id"]
    REGION = dataproc_configs["region"]

    # 3.GCS、Bigquery 相關變數
    gcs_bucket = Variable.get("GCS_BUCKET_NAME")
    staging_dataset = Variable.get("BIGQUERY_STAGING_DATASET")
    
    # === 第一階段：資料載入 ===
    
    # 1. 載入 Ubike 資料
    load_ubike_data_to_bigquery = GCSToBigQueryOperator(
        task_id="load_ubike_data_to_bigquery",
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
    
    # 2. 天氣資料處理流程 (dataproc 暫時叢集)

    # A. 建立 Dataproc 叢集
    create_cluster=DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        execution_timeout=timedelta(minutes=30),
    )

    # B. 提交 PySpark 工作給叢集

    # 複製 Job Config 並在 DAG 內部添加 Jinja 參數
    spark_job_with_args=SPARK_JOB_CONFIG.copy()
    spark_job_with_args["pyspark_job"]["args"]=[
        "{{data_interval_start.subtract(days=1).strftime('%Y/%m/%d')}}"
    ]

    submit_spark_job=DataprocSubmitJobOperator(
        task_id="submit_spark_weather_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=spark_job_with_args, #使用Job Config
        execution_timeout=timedelta(minutes=10),
    )

    # C. GCS to Bigquery
    load_weather_data_to_bigquery=GCSToBigQueryOperator(
        task_id="load_weather_data_to_bigquery",
        bucket=gcs_bucket,
        source_objects=[
            'weather_cleaned_parquet/'
            '{{ data_interval_start.subtract(days=1).strftime("%Y/%m/%d") }}'
            '/*.parquet'
        ], 
        destination_project_dataset_table=f"{staging_dataset}.raw_weather_data",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # D. 刪除 Dataproc 叢集
    delete_cluster=DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        # 不論上游成功與否，都會刪除叢集
        trigger_rule="all_done",
    )
    

    
    # === 第二階段：dbt 轉換（顯示詳細 lineage） ===
    
    # 3. 使用 DbtTaskGroup 顯示所有 dbt models 的詳細執行狀態
    dbt_models = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=PROJECT_CONFIG,
        profile_config=runtime_profile_config,
        execution_config=EXECUTION_CONFIG,
        operator_args={
            "install_deps": True,
            "full_refresh": False,
        },
    )
    
    # === 任務依賴設定 ===
    
    # 1. Dataproc生命週期
    create_cluster >> submit_spark_job  >> [load_weather_data_to_bigquery, delete_cluster] 

    # 2. 主要資料流的依賴關係
    [load_ubike_data_to_bigquery,load_weather_data_to_bigquery] >> dbt_models


# 實例化 DAG
integrated_data_pipeline()