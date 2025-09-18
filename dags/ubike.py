from airflow.sdk import asset,Asset,Context
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from datetime import datetime
from zoneinfo import ZoneInfo
import time 
import json
import pendulum

#GCS相關設定
GCS_CONN_ID="gcp_bucket"
gcs_bucket = Variable.get("GCS_BUCKET_NAME")

weather_data_asset = Asset("weather://data-updated")

@asset(
    schedule=weather_data_asset,
    uri="https://datacenter.taichung.gov.tw/swagger/OpenData/bc27c2f7-6ed7-4f1a-b3cc-1a3cc9cda34e",
    description="從 API 取得原始 Ubike 資料並直接存入 GCS，不做任何清洗或轉換。",
)
def fetch_ubike(self) -> dict[str]:

    max_retries = 5
    retry_delay = 30 # 秒
    for attempt in range(max_retries):
        try:
            import requests
            r=requests.get(self.uri)
            r.raise_for_status()
            raw_data=r.json()
            return raw_data

        except requests.exceptions.RequestException as e:
            print(f"第 {attempt + 1} 次嘗試失敗: {e}")
            # 如果這是最後一次嘗試，就拋出例外
            if attempt == max_retries - 1:
                print("已達到最大重試次數，任務將失敗。")
                raise
            else:
                print(f"等待 {retry_delay} 秒後再次嘗試...")
                time.sleep(retry_delay)


@asset(
    schedule=fetch_ubike
)
def load_ubike_data_to_gcs(context:Context,fetch_ubike:Asset):
    try:
        ubike_data=context["inlet_events"][fetch_ubike][-1].source_task_instance.xcom_pull(
        )
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        
        # 取得目前的日期和時間，並設定時區為台北
        now = datetime.now(ZoneInfo("Asia/Taipei"))
        # 建立階層式目錄結構
        # 格式為 'ubike_raw/年/月/日/時間戳.json'
        # 例如：'ubike_raw/2025/09/10/20250910_172300.json'
        file_path = now.strftime("ubike_raw/%Y/%m/%d/%Y%m%d_%H%M%S.json")

        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=file_path,
            data=json.dumps(ubike_data, ensure_ascii=False).encode('utf-8'),
            mime_type="application/json; charset=utf-8"
        )
        
        print(f"原始 Ubike 資料已成功上傳至 gs://{gcs_bucket}/{file_path}")
    except Exception as e:
        print(f"上傳資料至 GCS 失敗: {e}")
        raise