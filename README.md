# ✨ 整合式 ELT 管道架構 (Dataproc & dbt Cosmos)

本專案旨在建立一個穩固、高效且可監控的端到端 ELT (Extract, Load, Transform) 數據管道。我們使用 **Apache Airflow** 進行調度，利用 **Google Dataproc** 進行數據清洗，並採用 **dbt Cosmos** 框架實現數據倉儲的無縫建模。

## 🌊 Apache Airflow & Cosmos - ELT 協作層

本專案使用 **Apache Airflow** 進行端到端 ELT 流程的調度和監控。核心亮點是採用 **Cosmos** 框架，實現與 dbt 專案的無縫整合。

### 階段一：高頻數據採集與同步 (API $\to$ GCS)

此階段的 DAG 採用 **Asset (資料血緣) 驅動**模式，每 10 分鐘進行一次檢查與數據採集，確保數據的即時性。

| DAG / 檔案名稱 | 職責 (API $\to$ GCS) | 核心工程實踐 (排程邏輯) | 
 | ----- | ----- | ----- | 
| **`weather.py`** | **驅動源**：從 CWA API 抓取氣象數據到 GCS。 | **高頻率 (每 10 分鐘)**：使用 `task.sensor` 輪詢 CWA API 的時間戳，**只有在數據版本更新時才觸發**採集。任務成功後輸出 **`weather://data-updated` Asset**。 | 
| **`ubike.py`** | **同步採集**：從 Ubike API 抓取站點數據到 GCS。 | **事件驅動 (每 10 分鐘檢查)**：任務排程**直接依賴**於 `weather://data-updated` Asset。這確保了 Ubike 數據的採集**緊隨最新的 Weather 數據更新事件之後**。同時內建 **5 次 API 故障重試**機制，提高穩定性。 | 

### 階段二：低頻批次處理與數據轉換 ($\text{GCS} \to \text{BigQuery} \to \text{dbt}$)

此階段由單一 DAG (**`integrated_data_pipeline`**) 負責，**每日僅運行一次** (`0 5 * * *`)，用於數據清洗、載入和核心倉儲建模。

#### 核心管道：integrated_data_pipeline 任務流程

| 任務階段 | 程式碼 / $\text{Operator}$ | 職責與進階處理 | 
 | ----- | ----- | ----- | 
| **外部通知** | `SlackNotifier` | **即時監控：** 使用 `on_success_callback` 和 `on_failure_callback` 確保 DAG 執行結果透過 Slack 即時通知團隊。 | 
| **Dataproc 叢集生命週期** | `DataprocCreateClusterOperator` / `DataprocDeleteClusterOperator` | **動態資源管理：** 根據模組化配置，動態建立和刪除 Dataproc 暫時叢集。刪除操作使用 `trigger_rule="all_done"`，確保叢集釋放。 | 
| **天氣數據預清理 (E)** | `DataprocSubmitJobOperator` (PySpark) | **數據清洗：** 提交 PySpark 工作到 Dataproc 叢集。該工作負責解決 CWA 原始 JSON 格式中包含 **`@` 符號等不相容元素**。 | 
| **資料載入 (L)** | `GCSToBigQueryOperator` | **Staging 載入：** 將 GCS 中昨日的 Ubike 數據（JSON）和已清洗的 Weather 數據（Parquet），以 **`WRITE_TRUNCATE` 模式**覆蓋載入到 BigQuery Staging 層。 | 
| **數據轉換 (T)** | `DbtTaskGroup (Cosmos)` | **倉儲建模：** 執行 dbt 專案，將 Staging 數據轉換為核心 Analytics 模型。使用 **Cosmos** 實現 dbt 任務的 **細粒度 Airflow 血緣追蹤與可視化監控**。 | 

### 模組化與配置層 (I/O 隔離)

為了確保 DAG 載入階段的穩定性（即避免 Airflow Variables 讀取失敗導致 DAG 停止解析），所有配置和 I/O 操作均被推遲到獨立的模組化檔案中。

| 檔案名稱 | 核心職責 | 關鍵實踐 | 
 | ----- | ----- | ----- | 
| **`dags/integrated_data_pipeline.py`** | **主 DAG 定義** | **I/O 隔離**：頂層不再直接讀取 Airflow Variables，而是調用配置函式。只負責**任務排序**和 Jinja 模板參數 (例如 `{{ ds_nodash }}`) 的傳遞。 | 
| **`include/configs/dataproc_config.py`** | **Dataproc I/O 隔離與配置生成** | **配置動態化**：負責安全地讀取所有 Dataproc 相關 Airflow Variables，並基於這些變數動態生成可用的 `ClusterConfig` 物件和 PySpark Job Config 字典。 | 
| **`include/configs/dbt_cosmos_config.py`** | **dbt Cosmos 運行時配置** | **Profile 隔離**：負責在 DAG 運行時讀取 `PROJECT_ID` 和 `BIGQUERY_WAREHOUSE_DATASET` 等變數，動態產生 dbt 連線所需的 `ProfileConfig`。 | 
| **`airflow_settings.yaml`** | **Airflow 運行環境配置** | **配置連線**：定義 gcp 連線、Slack 連線，並設定 Airflow Variables (如 `PROJECT_ID`、`GCS_BUCKET` 等) 供模組化配置檔案使用。 | 
| **`Dockerfile`** | 建立 $\text{dbt}$ 專案所需的 $\text{Python}$ 虛擬環境。 | N/A | 
| **`requirements.txt`** | 安裝運行 $\text{Airflow}$ 任務所需的所有 $\text{Python}$ 套件。 | N/A |