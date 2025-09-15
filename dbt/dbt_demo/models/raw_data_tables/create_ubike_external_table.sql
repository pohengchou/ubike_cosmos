CREATE OR REPLACE EXTERNAL TABLE
  `ubike-471005.dbt_poheng.ubike_raw_external` (
    -- 根物件包含資料更新時間和狀態
    `update_time` STRING,
    `status` STRING,
    -- 觀測站資料的陣列
    `retVal` ARRAY<STRUCT<
      `sno` STRING,
      `sna` STRING,
      `tot` STRING,
      `sbi` STRING,
      `sarea` STRING,
      `ar` STRING,
      `bemp` STRING,
      `act` STRING,
      `mday` STRING,
      `lat` STRING,
      `lng` STRING
    >>
  ) OPTIONS (
    format = 'JSON',
    uris = ['gs://ubike-471005-data-lake/ubike_raw/*/*/*/*.json'],
    hive_partitioning_mode = 'AUTO'
  );
