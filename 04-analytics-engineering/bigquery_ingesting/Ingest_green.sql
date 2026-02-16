-- Create temporary external table monthly
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-project-485916.zoomcamp.temp_green`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-taha-demo/green_tripdata_2020-*.parquet']
);

-- Insert with explicit casting
INSERT INTO `zoomcamp-project-485916.zoomcamp.green_taxi_trips`
SELECT 
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    CAST(ehail_fee AS FLOAT64) AS ehail_fee,  -- Cast INT to FLOAT
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM `zoomcamp-project-485916.zoomcamp.temp_green`;

-- Clean up
DROP TABLE `zoomcamp-project-485916.zoomcamp.temp_green`;