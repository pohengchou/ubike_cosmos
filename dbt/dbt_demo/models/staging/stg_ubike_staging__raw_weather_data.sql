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
        NULLIF(CAST(GeoInfo.TownCode AS INT64), -99) AS town_code,   
        NULLIF(CAST(GeoInfo.CountyCode AS INT64), -99) AS county_code, 
        NULLIF(CAST(GeoInfo.StationAltitude AS FLOAT64), -99) AS station_altitude,
        
        coordinate.CoordinateName AS coordinate_name,
        NULLIF(CAST(coordinate.StationLatitude AS FLOAT64), -99) AS latitude,
        NULLIF(CAST(coordinate.StationLongitude AS FLOAT64), -99) AS longitude,
        
        ObsTime.DateTime AS observation_timestamp,
        
        NULLIF(WeatherElement.VisibilityDescription, '-99') AS visibility_description,
        NULLIF(CAST(WeatherElement.AirTemperature AS FLOAT64), -99) AS air_temperature,
        NULLIF(CAST(WeatherElement.AirPressure AS FLOAT64), -99) AS air_pressure,
        NULLIF(CAST(WeatherElement.RelativeHumidity AS FLOAT64), -99) AS relative_humidity,
        NULLIF(CAST(WeatherElement.WindSpeed AS FLOAT64), -99) AS wind_speed,
        NULLIF(CAST(WeatherElement.WindDirection AS FLOAT64), -99) AS wind_direction,
        NULLIF(CAST(WeatherElement.UVIndex AS INT64), -99) AS uv_index,
        NULLIF(CAST(WeatherElement.SunshineDuration AS FLOAT64), -99) AS sunshine_duration,
        NULLIF(CAST(WeatherElement.NOW.Precipitation AS FLOAT64), -99) AS precipitation,
        NULLIF(WeatherElement.Weather, '-99') AS weather_description,
        
        NULLIF(CAST(WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.AirTemperature AS FLOAT64), -99) AS daily_low_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS daily_low_temp_at,
        
        NULLIF(CAST(WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.AirTemperature AS FLOAT64), -99) AS daily_high_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS daily_high_temp_at,
        
        NULLIF(CAST(WeatherElement.GustInfo.PeakGustSpeed AS FLOAT64), -99) AS peak_gust_speed,
        NULLIF(CAST(WeatherElement.GustInfo.Occurred_at.WindDirection AS FLOAT64), -99) AS gust_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.GustInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS gust_occurred_at,
        
        NULLIF(CAST(WeatherElement.Max10MinAverage.WindSpeed AS FLOAT64), -99) AS max_10min_avg_wind_speed,
        NULLIF(CAST(WeatherElement.Max10MinAverage.Occurred_at.WindDirection AS FLOAT64), -99) AS max_10min_avg_wind_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.Max10MinAverage.Occurred_at.DateTime AS STRING), '-99')
        ) AS max_10min_avg_wind_at
        
    FROM
        source,
        UNNEST(source.GeoInfo.Coordinates) AS coordinate
)

SELECT * FROM flattened
WHERE coordinate_name = 'WGS84'
