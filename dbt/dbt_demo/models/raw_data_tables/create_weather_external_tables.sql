CREATE OR REPLACE EXTERNAL TABLE
  `ubike-471005.dbt_poheng.weather_raw_external` (
    `cwaopendata` STRUCT<
      `sent` STRING,
      `scope` STRING,
      `@xmlns` STRING,
      `dataid` STRING,
      `sender` STRING,
      `status` STRING,
      `dataset` STRUCT<
        `Station` ARRAY<STRUCT<
          `GeoInfo` STRUCT<
            `TownCode` STRING,
            `TownName` STRING,
            `CountyCode` STRING,
            `CountyName` STRING,
            `Coordinates` ARRAY<STRUCT<
              `CoordinateName` STRING,
              `StationLatitude` STRING,
              `CoordinateFormat` STRING,
              `StationLongitude` STRING
            >>,
            `StationAltitude` STRING
          >,
          `ObsTime` STRUCT<
            `DateTime` STRING
          >,
          `StationId` STRING,
          `StationName` STRING,
          `WeatherElement` STRUCT<
            `Now` STRUCT<
              `Precipitation` STRING
            >,
            `UVIndex` STRING,
            `Weather` STRING,
            `GustInfo` STRUCT<
              `Occurred_at` STRUCT<
                `DateTime` STRING,
                `WindDirection` STRING
              >,
              `PeakGustSpeed` STRING
            >,
            `WindSpeed` STRING,
            `AirPressure` STRING,
            `DailyExtreme` STRUCT<
              `DailyLow` STRUCT<
                `TemperatureInfo` STRUCT<
                  `Occurred_at` STRUCT<
                    `DateTime` STRING
                  >,
                  `AirTemperature` STRING
                >
              >,
              `DailyHigh` STRUCT<
                `TemperatureInfo` STRUCT<
                  `Occurred_at` STRUCT<
                    `DateTime` STRING
                  >,
                  `AirTemperature` STRING
                >
              }
            >,
            `WindDirection` STRING,
            `AirTemperature` STRING,
            `RelativeHumidity` STRING,
            `SunshineDuration` STRING,
            `VisibilityDescription` STRING,
            `Max10MinAverage` STRUCT<
              `WindSpeed` STRING,
              `Occurred_at` STRUCT<
                `DateTime` STRING,
                `WindDirection` STRING
              >
            >
          >
        >>
      >
    >
  ) OPTIONS (
    format = 'JSON',
    uris = ['gs://ubike-471005-data-lake/weather_raw/*/*/*/*.json'],
    hive_partitioning_mode = 'AUTO'
  );