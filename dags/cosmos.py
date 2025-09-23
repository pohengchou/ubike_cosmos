from cosmos import ProjectConfig, ProfileConfig,ExecutionConfig,DbtDag
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
import os
from datetime import datetime

DBT_PROJECT_PATH="/usr/local/airflow/dbt/dbt_demo"
DBT_EXECUTABLE_PATH=f"{os.getenv('AIRFLOW_HOME')}/ubike_cosmos/bin/dbt"
BIGQUERY_DATASET="ubike_471005_data_warehouse"
KEYFILE_PATH= "/usr/local/airflow/include/keys/service_account.json"

_project_config=ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH
)

_profile_config = ProfileConfig(
    profile_name="dbt_demo",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id ="Bigquery",
        profile_args = { "project":"ubike-471005","dataset": "ubike_471005_data_warehouse","keyfile": KEYFILE_PATH}
    )
)

_execution_config=ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

my_dag=DbtDag(
    dag_id='my_dag',
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    schedule="0 6 * * *",
    start_date=datetime(2025,1,1),
    max_active_tasks=2
)