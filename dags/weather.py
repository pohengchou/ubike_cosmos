from airflow.sdk import dag,task,Asset
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.models.variable import Variable
from datetime import datetime
from zoneinfo import ZoneInfo
import json

#GCS相關設定
GCS_BUCKET_NAME="ubike-471005-data-lake"
GCS_CONN_ID="gcp_bucket"

#天氣API相關設定
WEATHER_API="https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0003-001?Authorization=CWA-E452AA68-C52D-4D00-8C1B-314DEF3F5DA2&downloadType=WEB&format=JSON"
API_KEY="CWA-E452AA68-C52D-4D00-8C1B-314DEF3F5DA2"
LAST_UPDATE_VAR_KEY = "weather_last_update_time"

# 定義 Weather Asset - 這將作為觸發信號
weather_data_asset = Asset("weather://data-updated")

@dag(
    schedule="*/10 * * * *"
)
def fetch_weather():
    @task.sensor(poke_interval=30,timeout=600)
    def is_api_available(task_instance=None)-> PokeReturnValue:
        import requests

        # 從 Airflow Variable 中取得上次記錄的更新時間
        last_update_time = Variable.get(LAST_UPDATE_VAR_KEY, default_var=None)

        params={"Authorization":API_KEY}
        r=requests.get(WEATHER_API,params=params)
        print(r.status_code)
        if r.status_code==200:
            condition=True
            weather_data=r.json()

            new_update_time = weather_data['cwaopendata']['sent']
            print(f"上次記錄的更新時間：{last_update_time}")
            print(f"API 回傳的最新時間：{new_update_time}")
            if last_update_time is None or new_update_time > last_update_time:
                print("偵測到新的資料版本，Sensor 條件達成。")
                return PokeReturnValue(is_done=True, xcom_value=weather_data)
            else:
                print("資料尚未更新，繼續等待...")
                # 條件未滿足，回傳 is_done=False，Sensor 會再次探測。
                return PokeReturnValue(is_done=False)
        else:
            condition=False
            weather_data=None
            return PokeReturnValue(is_done=False)

    
    @task(outlets=[weather_data_asset])
    def store_weather_data(weather_data,task_instance=None):

        new_update_time = weather_data['cwaopendata']['sent']
         # 將最新的時間寫入 Airflow Variable，供下次 DAG 執行時使用
        Variable.set(LAST_UPDATE_VAR_KEY, new_update_time)
        # 取得目前的日期和時間，並設定時區為台北
        now = datetime.now(ZoneInfo("Asia/Taipei"))
        # 建立階層式目錄結構
        # 格式為 'ubike_raw/年/月/日/時間戳.json'
        # 例如：'ubike_raw/2025/09/10/20250910_172300.json'
        file_path = now.strftime("weather_raw/%Y/%m/%d/%Y%m%d_%H%M%S.json")
        gcs_hook=GCSHook(gcp_conn_id=GCS_CONN_ID)
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=file_path,
            data=json.dumps(weather_data,ensure_ascii=False).encode('utf-8'),
            mime_type="application/json; charset=utf-8"
        )

    store_weather_data(is_api_available())

fetch_weather()