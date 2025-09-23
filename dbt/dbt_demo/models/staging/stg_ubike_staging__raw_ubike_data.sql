{{ config(materialized='view') }}

WITH source AS (
  SELECT *
  FROM {{ source('ubike_staging', 'raw_ubike_data') }}
),

unnested AS (
  SELECT
    source.updated_at,
    unnested_retVal.*
  FROM source
  , UNNEST(source.retVal) AS unnested_retVal
),

renamed AS (
  SELECT
    CAST(sno AS INT64) AS station_id,
    sna AS station_name,
    sarea AS district,
    lat AS latitude,
    lng AS longitude,
    updated_at AS api_request_at,
    PARSE_DATETIME('%Y%m%d%H%M%S', CAST(mday AS STRING)) AS data_updated_at,
    CAST(sbi AS INT64) AS available_bikes,
    CAST(bemp AS INT64) AS empty_docks,
    CAST(tot AS INT64) AS total_docks,
    ar AS address,
    act AS is_active,
    CAST(sbi_detail.eyb AS INT64) AS electric_bikes,
    CAST(sbi_detail.yb2 AS INT64) AS normal_bikes
  FROM unnested
)

SELECT * FROM renamed