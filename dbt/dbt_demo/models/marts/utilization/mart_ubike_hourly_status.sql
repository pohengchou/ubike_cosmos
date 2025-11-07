{{
    config(
        materialized='table',
        partition_by={
            'field':'date',
            'data_type':'date'
        },
        cluster_by=['station_id_key','hour_of_day']
    )
}}


WITH clean_data AS(
    SELECT
        *,
        FORMAT_DATE('%A', date) AS day_of_week
    FROM{{ref('hourly_status')}}
),

station_metrics AS(
    SELECT
        date,
        hour_of_day,
        day_of_week,
        station_id_key,
        total_docks,

        --KPI: 平均利用率
        AVG(available_bikes/total_docks) AS avg_utilization_rate,
        SUM(CASE WHEN available_bikes<=0 THEN 1 ELSE 0 END) AS count_zero_bikes_absolute,
        SUM(CASE WHEN empty_docks<=0 THEN 1 ELSE 0 END) AS count_zero_docks_absolute,
        
        SUM(CASE WHEN available_bikes <= 2 THEN 1 ELSE 0 END) AS count_low_bikes_risk,
        SUM(CASE WHEN empty_docks <= 2 THEN 1 ELSE 0 END) AS count_low_docks_risk,


        COUNT(1) AS total_observations_count
    FROM clean_data
    GROUP BY 
        1,2,3,4,5
    HAVING 
        total_observations_count>0
)

SELECT
    T1.station_id_key,
    T1.date,
    T1.hour_of_day,
    T1.day_of_week,
    t1.total_docks,

    T2.district,
    T2.station_name,
    CONCAT(CAST(T2.latitude AS STRING),',',CAST(T2.longitude AS STRING)) AS geo_coordinates,

    T1.avg_utilization_rate,
    SAFE_DIVIDE(T1.count_zero_bikes_absolute, T1.total_observations_count) AS zero_bikes_ratio,
    SAFE_DIVIDE(T1.count_zero_docks_absolute, T1.total_observations_count) AS zero_docks_ratio,

    SAFE_DIVIDE(T1.count_low_bikes_risk, T1.total_observations_count) AS low_bikes_ratio,
    SAFE_DIVIDE(T1.count_low_docks_risk, T1.total_observations_count) AS low_docks_ratio,
    

    
    T1.total_observations_count

FROM
    station_metrics AS T1
LEFT JOIN
    {{ref('dim_ubike_stations')}} AS T2
    ON T1.station_id_key= T2.station_id
