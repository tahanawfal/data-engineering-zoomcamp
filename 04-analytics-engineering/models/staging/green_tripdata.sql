{{ config(materialized='view') }}

SELECT * FROM {{ source('raw_data', 'external_green_taxi') }}
WHERE lpep_pickup_datetime >= '2019-01-01'
  AND lpep_pickup_datetime < '2019-02-01'