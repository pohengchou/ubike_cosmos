{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "observation_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['weather_station_id_key']
) }}

SELECT
   -- 代理鍵：確保每筆觀測記錄的唯一性
    GENERATE_UUID() AS surrogate_key,
    station_id AS weather_station_id_key,
    observation_timestamp,

    -- 可量化指標（事實）：各種天氣數據
    air_temperature,
    air_pressure,
    relative_humidity,
    wind_speed,
    precipitation,
    uv_index,
    sunshine_duration,

    -- 描述性事實
    weather_description,
    wind_direction,
    visibility_description,

    -- 每日極值
    daily_low_temp,
    daily_high_temp


FROM {{ref('stg_ubike_staging__raw_weather_data')}}

{% if is_incremental() %}
    -- 增量策略的關鍵部分。
    -- 只處理 observation_timestamp 比事實表中現有最大時間戳還新的資料，
    -- 避免重複處理舊資料，提高運行效率。
    WHERE observation_timestamp > (SELECT MAX(observation_timestamp) FROM {{ this }})
{% endif %}