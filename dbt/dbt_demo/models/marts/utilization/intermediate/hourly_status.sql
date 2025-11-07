{{
    config(materialized='view')
}}

--1. fact_ubike_status.sql 增加一'rn'欄位，依照api_request_at最舊到最新排序
WITH rn_table AS(
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY station_id_key,data_updated_at
            ORDER BY api_request_at 
        ) as rn
    FROM {{ref('fact_ubike_status')}}
)


SELECT
    DATE(data_updated_at) AS date,
    EXTRACT(HOUR FROM data_updated_at) AS hour_of_day,
    station_id_key,
    available_bikes,
    empty_docks,
    total_docks
FROM
    rn_table
WHERE 
    rn=1
    AND is_active=1
    AND total_docks > 0
