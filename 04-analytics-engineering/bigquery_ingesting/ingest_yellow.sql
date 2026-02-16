-- Create temporary external table monthly
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-project-485916.zoomcamp.temp_yellow`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-taha-demo/yellow_tripdata_2019-*.parquet']
);

-- Insert with explicit casting
INSERT INTO `zoomcamp-project-485916.zoomcamp.yellow_taxi_trips`
SELECT 
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    CAST(airport_fee AS FLOAT64) AS airport_fee   -- Cast INT to FLOAT
FROM `zoomcamp-project-485916.zoomcamp.temp_yellow`;

-- Clean up
DROP TABLE `zoomcamp-project-485916.zoomcamp.temp_yellow`;