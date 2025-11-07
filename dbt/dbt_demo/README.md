## ⚙️ dbt (Data Build Tool) - Ubike & Weather 數據倉儲

本專案使用 **dbt** 在 **BigQuery** 中實施數據轉換，遵循 **Kimball 維度建模原則**，建立 **Star Schema (星型結構)**，並確保數據品質。

### 核心模型與優化策略

| 檔案名稱 / 模型類型 | 實體化策略 | 核心工程亮點 (BigQuery 優化與建模) |
| :--- | :--- | :--- |
| **`fact_ubike_status`** | **增量更新** | ubike事實表。使用 **`api_request_at` 按日分區**，按 `station_id_key` **聚類**，大幅優化查詢。 |
| **`fact_weather`** | **增量更新** | 氣象事實表。使用 **`observation_timestamp` 按日分區**，確保高效率地處理新數據。 |
| **`dim_ubike_stations`** | **維度表** | 包含 $\text{Ubike}$ 站點的靜態資訊。 |
| **`dim_weather_stations`** | **維度表** | 包含氣象站的地理資訊，使用 **`county_name` 字串分區**。 |

### 數據品質與預處理

| 檔案名稱 / 數據層級 | 核心工程實踐 (Data Quality & Staging) |
| :--- | :--- |
| **`sources.yml`** | **數據血緣**：定義原始 $\text{schema}$，包含巢狀結構，並設置**業務級 $\text{tests}$** (如濕度範圍 $\text{0\%-100\%}$)。 |
| **`stg_*.sql`** | **數據清洗**：使用 $\text{UNNEST}$ **攤平 $\text{API}$ 數據**，將 $\text{-99}$ 替換為 $\text{NULL}$，進行數據標準化。 |
| **$\text{dbt Tests}$** | **關係完整性**：實施 $\text{relationships}$ 測試，確保 $\text{Fact}$ 表外來鍵**完整存在**於維度表中。 |