import logging
from typing import Final, Dict, Any
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from google.cloud.dataproc_v1.types import Cluster

# 設定日誌
log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# 1. 靜態鍵名定義 (不進行任何 I/O 操作)
# ------------------------------------------------------------------
VAR_PROJECT_ID: Final[str] = "PROJECT_ID"
VAR_REGION: Final[str] = "GCP_REGION"
VAR_SERVICE_ACCOUNT: Final[str] = "SERVICE_ACCOUNT_EMAIL"
VAR_PYSPARK_JOB_FILE_PATH: Final[str] = "PYSPARK_JOB_FILE_PATH"
VAR_AUTOSCALING_POLICY_ID: Final[str] = "AUTOSCALING_POLICY_ID"


# ------------------------------------------------------------------
# 2. I/O 隔離與錯誤處理函式
# ------------------------------------------------------------------
def get_variable_or_default(key:str, default:Any, log_level:str='warning')-> Any:
    """
    安全地從airflow Variable獲取值，失敗時返回預設值。
    Args:
        key: Airflow Variable 的鍵名。
        default: 找不到變數時的回傳值。
        log_level: 日誌的級別。("warning" or "error" )

    目的: 確保匯入失敗時，仍呈現Dag在 UI介面上。
    """

    try:
        return Variable.get(key)
    except Exception as e:
        log_function=log.warning if log_level=="warning" else log.error
        log_function(f"無法獲取 Airflow Variable '{key}':{e}. 使用預設值:{default}")
        return default


# ------------------------------------------------------------------
# 3. 配置生成主函式 (運行時在 DAG 內部調用)
# ------------------------------------------------------------------
def generate_dataproc_configs(ds_nodash: str) -> Dict[str, Any]:
    """
    在DAG運行時讀取所有， Dataproc 相關變數，並生成叢集與 Job配置。

    Args:
        ds_nodash: 當前執行日期的字串(YYYYMMDD)，用於動態命名叢集。

    Returns:
        包含所有 Dataproc 配置的字典。
    """
    # 動態叢集名稱
    CLUSTER_NAME: str= f"dataproc-weather-cluster-{ds_nodash}"

    # 讀取所有 Airflow 變數
    PROJECT_ID: str=get_variable_or_default(VAR_PROJECT_ID,"gcp-project-placeholder",log_level='error')
    REGION: str=get_variable_or_default(VAR_REGION,"us-central1")
    SERVICE_ACCOUNT: str=get_variable_or_default(
        VAR_SERVICE_ACCOUNT,
        "dataproc-sa@gcp-project-placeholder.iam.gserviceaccount.com",
        log_level='error'
    )
    PYSPARK_JOB_FILE_PATH: str=get_variable_or_default(VAR_PYSPARK_JOB_FILE_PATH, "gs://placeholder-bucket/pyspark/clean_weather.py")
    AUTOSCALING_POLICY_ID: str=get_variable_or_default(VAR_AUTOSCALING_POLICY_ID,"default-autoscaling-policy")

    #自動調度政策URI
    AUTOSCALING_POLICY_URI: str=(
        f"projects/{PROJECT_ID}/locations/{REGION}/autoscalingPolicies/{AUTOSCALING_POLICY_ID}"
    )

    # 1. Dataproc叢集配置
    cluster_config_generator= ClusterGenerator(
        project_id=PROJECT_ID,
        # 節點配置
        master_machine_type="e2-standard-8",
        worker_machine_type="e2-standard-8",
        num_workers=2,
        master_disk_size=100,
        worker_disk_size=100,
        # 自動調度配置：Generator 會將這個 URI 放置在正確的 Protobuf 欄位中 (autoscalingConfig)
        autoscaling_policy=AUTOSCALING_POLICY_URI,
        # GCE 配置
        service_account=SERVICE_ACCOUNT,
        tags=["dataproc", "airflow"],
        # 軟體配置
        image_version="2.2-debian12",
        properties={
            # 這是允許 worker 數目縮減到零的屬性
            "dataproc:dataproc.allow.zero.workers":"true"
        },
    )
    # 生成最終的 Cluster object
    CLUSTER_CONFIG: Cluster = cluster_config_generator.make()


    # 2. PySpark 工作配置
    SPARK_JOB_CONFIG: Dict[str, Any]={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},  # 提交工作到指定的 cluster
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_JOB_FILE_PATH,
            "properties": {
                "spark.executor.memory": "16g",
                "spark.executor.cores": "4",
                "spark.driver.memory": "4g",
                "spark.default.parallelism": "24",
            },
            # DAG 運行時會動態傳入日期參數，此處留空
            "args": [], 
        },
    }


    # 3. 回傳所有配置
    return {
        "project_id": PROJECT_ID,
        "region": REGION,
        "cluster_name": CLUSTER_NAME,
        "cluster_config": CLUSTER_CONFIG,
        "spark_job_config": SPARK_JOB_CONFIG
    }

