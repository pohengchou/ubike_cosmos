{{config(
    materialized='view'
)}}

WITH source_weather AS(
    SELECT
        *
    FROM {{ref('fact_weather')}}
),

impute_locf AS(
    SELECT
        *,
        LAST_VALUE(air_pressure IGNORE NULLS) OVER (
            PARTITION BY weather_station_id_key
            ORDER BY observation_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS air_pressure_imputed,
        LAST_VALUE(uv_index  IGNORE NULLS) OVER (
            PARTITION BY weather_station_id_key
            ORDER BY observation_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS uv_index_imputed,
        LAST_VALUE(sunshine_duration IGNORE NULLS) OVER (
            PARTITION BY weather_station_id_key
            ORDER BY observation_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sunshine_duration_imputed,
        LAST_VALUE(visibility_description IGNORE NULLS) OVER (
            PARTITION BY weather_station_id_key
            ORDER BY observation_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS visibility_description_imputed,
    
    FROM
        source_weather
)

SELECT
    surrogate_key,
    weather_station_id_key,
    observation_timestamp,
    
    -- 使用插補值，如果原始值為 NULL
    air_temperature,
    COALESCE(air_pressure, air_pressure_imputed) AS air_pressure_final,
    relative_humidity,
    wind_speed,
    precipitation,
    COALESCE(uv_index, uv_index_imputed) AS uv_index_final,
    COALESCE(sunshine_duration, sunshine_duration_imputed) AS sunshine_duration_final,
    
    weather_description,
    wind_direction,
    
    -- 最終使用插補後的能見度描述
    COALESCE(visibility_description, visibility_description_imputed) AS visibility_description_final,

    daily_low_temp,
    daily_high_temp
FROM
    impute_locf
    