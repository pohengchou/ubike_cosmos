{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "api_request_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['station_id_key']
) }}

SELECT
    -- 使用 FARM_FINGERPRINT 將多個欄位組合成一個唯一的代理鍵
    FARM_FINGERPRINT(
        CONCAT(
            CAST(station_id AS STRING),
            CAST(api_request_at AS STRING)
        )
    ) AS Surrogate_Key,
    -- 外來鍵，用於連接維度表
    station_id AS station_id_key,
    api_request_at,
    data_updated_at,
    available_bikes,
    empty_docks,
    total_docks,
    electric_bikes,
    normal_bikes,
    is_active
FROM
    {{ ref('stg_ubike_staging__raw_ubike_data') }} 
{% if is_incremental() %}
    -- 這是增量策略的關鍵部分。
    -- 只有當 dbt 運行在增量模式下，這段程式碼才會被執行。
    -- 它會篩選出 `api_request_at` 比目標表中現有最大時間戳還新的資料。
    -- 這樣可以確保每次只處理新資料，大大提升效率。
    WHERE api_request_at > (SELECT MAX(api_request_at) FROM {{ this }})
{% endif %}