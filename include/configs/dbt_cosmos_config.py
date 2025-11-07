import os
import logging
from typing import Final
from airflow.models import Variable
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# 設定日誌
log = logging.getLogger(__name__)

# 1. 靜態配置常數
DBT_PROJECT_PATH:Final[str] = "/usr/local/airflow/dbt/dbt_demo"
DBT_EXECUTABLE_PATH:Final[str] = f"{os.getenv('AIRFLOW_HOME')}/ubike_cosmos/bin/dbt"
KEYFILE_PATH:Final[str] = "/usr/local/airflow/include/keys/service_account.json"
DBT_TARGET_NAME:Final[str]="dev"
DBT_PROFILE_NAME:Final[str]="dbt_demo"

# 2. 靜態配置物件
PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH
)


EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

# 3. 動態配置物件(隔離I/O)
def get_runtime_profile_config() -> ProfileConfig:
    """
    獲取 ProfileConfig
    """
    PROJECT_ID = None
    BIGQUERY_DATAWAREHOUSE_DATASET = None

    try:
        PROJECT_ID=Variable.get("PROJECT_ID")
        BIGQUERY_DATAWAREHOUSE_DATASET=Variable.get("BIGQUERY_DATAWAREHOUSE_DATASET")
    except Exception as e:
        log.error(f"無法獲取 Airflow Variable:{e}")
        PROJECT_ID="PLACEHOLDER_PROJECT"
        BIGQUERY_DATAWAREHOUSE_DATASET="PLACEHOLDER_DATAWAREHOUSE"
    return ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name=DBT_TARGET_NAME,
        profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
            conn_id="Bigquery",
            profile_args={
                "project": PROJECT_ID,
                "dataset": BIGQUERY_DATAWAREHOUSE_DATASET, 
                "keyfile": KEYFILE_PATH
            }
        )
    )