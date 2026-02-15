{{ config(materialized='view') }}

SELECT * FROM {{ source('raw_data', 'green_taxi_trips') }}
WHERE lpep_pickup_datetime >= '2019-01-01'
  AND lpep_pickup_datetime < '2019-02-01'