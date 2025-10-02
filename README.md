## 🌊 Apache Airflow & Cosmos - ELT 協作層

本專案使用 **Apache Airflow** 進行端到端 ELT 流程的調度和監控。核心亮點是採用 **Cosmos** 框架，實現與 $\text{dbt}$ 專案的無縫整合。

---

### 階段一：高頻數據採集與同步 (API $\to$ GCS)

此階段的 $\text{DAG}$ **每 10 分鐘**進行一次檢查與數據採集，確保數據的即時性。

| DAG / 檔案名稱 | 職責 (API $\to$ GCS) | 核心工程實踐 (排程邏輯) |
| :--- | :--- | :--- |
| **`weather.py`** | **驅動源**：從 $\text{CWA API}$ 抓取氣象數據到 $\text{GCS}$。 | **高頻率 (每 10 分鐘)**：使用 $\text{task.sensor}$ 輪詢 $\text{CWA API}$ 的時間戳，**只有在數據版本更新時才觸發**採集。任務成功後輸出 **`weather://data-updated` Asset**。 |
| **`ubike.py`** | **同步採集**：從 $\text{Ubike API}$ 抓取站點數據到 $\text{GCS}$。 | **事件驅動 (每 10 分鐘檢查)**：任務排程**直接依賴**於 `weather://data-updated` Asset。這確保了 $\text{Ubike}$ 數據的採集**緊隨最新的 $\text{Weather}$ 數據更新事件之後**。同時內建 **5 次 API 故障重試**機制，提高穩定性。 |

---

### 階段二：低頻批次處理與數據轉換 ($\text{GCS} \to \text{BigQuery} \to \text{dbt}$)

此階段的 $\text{DAG}$ **每日僅運行一次** (`0 5 * * *`)，用於數據清洗、載入和核心倉儲建模。

| 任務階段 | 程式碼 / $\text{Operator}$ | 職責與進階處理 |
| :--- | :--- | :--- |
| **天氣數據預清理** | $\text{@task.expand}$ + $\text{GCSHook}$ | **每日批次處理**：解決 $\text{CWA}$ 原始 $\text{JSON}$ 格式中包含 **$\text{@}$ 符號等不相容元素**，造成 $\text{BigQuery}$ 載入失敗的問題。使用 **$\text{expand}$** 功能**動態並行讀取多個 GCS 檔案**，並在 $\text{GCS}$ 上完成**頂層 $\text{JSON}$ 結構清理與攤平**。 |
| **資料載入 ($\text{L}$)** | $\text{GCSToBigQueryOperator}$ | **每日批次處理**：將 $\text{GCS}$ 中昨日的 $\text{Ubike}$ 和已清理的 $\text{Weather}$ 檔案，**覆蓋載入**到 $\text{BigQuery Staging}$ 層。 |
| **數據轉換 ($\text{T}$)** | $\text{DbtTaskGroup (Cosmos)}$ | **每日批次處理**：利用 $\text{Cosmos}$ 執行 $\text{dbt}$ 建模，將 $\text{Staging}$ 數據轉換為核心 $\text{Analytics}$ 模型，實現**細粒度血緣追蹤**和 $\text{Airflow UI}$ 上的**可視化監控**。 |

---

### 容器與環境配置

| 檔案名稱 | 核心職責 |
| :--- | :--- |
| **`Dockerfile`** | 建立 $\text{dbt}$ 專案所需的 $\text{Python}$ 虛擬環境。 |
| **`requirements.txt`** | 安裝運行 $\text{Airflow}$ 任務所需的所有 $\text{Python}$ 套件。 |
| **`airflow_settings.yaml`** | **配置連線**：定義 $\text{gcp\_bucket}$ 連線 $\text{ID}$，並安全地將 $\text{GCS\_BUCKET\_NAME}$ 等敏感資訊儲存在 **$\text{Airflow Variables}$** 中，供 $\text{DAG}$ 使用。 |