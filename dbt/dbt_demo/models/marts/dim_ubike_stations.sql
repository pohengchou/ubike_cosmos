SELECT  
    station_id,
    station_name,
    district,
    address,
    latitude,
    longitude,
    

FROM {{ref('stg_ubike_staging__raw_ubike_data')}}

GROUP BY
    station_id,
    station_name,
    district,
    address,
    latitude,
    longitude