{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('ubike_staging', 'raw_weather_data') }}
),

flattened AS (
    SELECT
        StationId AS station_id,
        StationName AS station_name,
        
        GeoInfo.TownName AS town_name,
        GeoInfo.CountyName AS county_name,
        
        NULLIF(SAFE_CAST(GeoInfo.TownCode AS INT64), -99) AS town_code,   
        NULLIF(SAFE_CAST(GeoInfo.CountyCode AS INT64), -99) AS county_code, 
        NULLIF(SAFE_CAST(GeoInfo.StationAltitude AS FLOAT64), -99) AS station_altitude,
        
        GeoInfo.Coordinates.list[0].element.CoordinateName AS coordinate_name, 
        NULLIF(SAFE_CAST(GeoInfo.Coordinates.list[0].element.StationLatitude AS FLOAT64), -99) AS latitude,
        NULLIF(SAFE_CAST(GeoInfo.Coordinates.list[0].element.StationLongitude AS FLOAT64), -99) AS longitude,
        
        SAFE_CAST(ObsTime.DateTime AS TIMESTAMP)AS observation_timestamp,
        
        NULLIF(WeatherElement.VisibilityDescription, '-99') AS visibility_description,
        
        NULLIF(SAFE_CAST(WeatherElement.AirTemperature AS FLOAT64), -99) AS air_temperature,
        NULLIF(SAFE_CAST(WeatherElement.AirPressure AS FLOAT64), -99) AS air_pressure,
        NULLIF(SAFE_CAST(WeatherElement.RelativeHumidity AS FLOAT64), -99) AS relative_humidity,
        NULLIF(SAFE_CAST(WeatherElement.WindSpeed AS FLOAT64), -99) AS wind_speed,
        NULLIF(SAFE_CAST(WeatherElement.WindDirection AS FLOAT64), -99) AS wind_direction,
        NULLIF(SAFE_CAST(WeatherElement.UVIndex AS INT64), -99) AS uv_index,
        NULLIF(SAFE_CAST(WeatherElement.SunshineDuration AS FLOAT64), -99) AS sunshine_duration,
        NULLIF(SAFE_CAST(WeatherElement.NOW.Precipitation AS FLOAT64), -99) AS precipitation,
        NULLIF(WeatherElement.Weather, '-99') AS weather_description,
        
        NULLIF(SAFE_CAST(WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.AirTemperature AS FLOAT64), -99) AS daily_low_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS daily_low_temp_at,
        
        NULLIF(SAFE_CAST(WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.AirTemperature AS FLOAT64), -99) AS daily_high_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS daily_high_temp_at,
        
        NULLIF(SAFE_CAST(WeatherElement.GustInfo.PeakGustSpeed AS FLOAT64), -99) AS peak_gust_speed,
        NULLIF(SAFE_CAST(WeatherElement.GustInfo.Occurred_at.WindDirection AS FLOAT64), -99) AS gust_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.GustInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS gust_occurred_at,
        
        NULLIF(SAFE_CAST(WeatherElement.Max10MinAverage.WindSpeed AS FLOAT64), -99) AS max_10min_avg_wind_speed,
        NULLIF(SAFE_CAST(WeatherElement.Max10MinAverage.Occurred_at.WindDirection AS FLOAT64), -99) AS max_10min_avg_wind_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.Max10MinAverage.Occurred_at.DateTime AS STRING), '-99')
        ) AS max_10min_avg_wind_at
        
    FROM
        source
)

SELECT * FROM flattened