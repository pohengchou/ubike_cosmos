{{config(
    partition_by={
        "field": "county_name",
        "data_type": "string"
    },
    cluster_by=['town_name','station_name']
)}}

SELECT
    station_id,
    station_name,
    county_name,
    town_name,
    station_altitude,
    latitude,
    longitude


FROM {{ref('stg_ubike_staging__raw_weather_data')}}


GROUP BY
    station_id,
    station_name,
    county_name,
    town_name,
    station_altitude,
    latitude,
    longitude