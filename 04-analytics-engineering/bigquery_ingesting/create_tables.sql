-- Create Table `green_taxi_trips`
CREATE OR REPLACE TABLE `zoomcamp-project-485916.zoomcamp.green_taxi_trips` (
  VendorID INT64,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag STRING,
  RatecodeID FLOAT64,
  PULocationID INT64,
  DOLocationID INT64,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type FLOAT64,
  trip_type FLOAT64,
  congestion_surcharge FLOAT64
);

-- Create Table `yellow_taxi_trips`
CREATE OR REPLACE TABLE `zoomcamp-project-485916.zoomcamp.yellow_taxi_trips` (
  VendorID INT64,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  RatecodeID FLOAT64,
  store_and_fwd_flag STRING,
  PULocationID INT64,
  DOLocationID INT64,
  payment_type INT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  congestion_surcharge FLOAT64,
  airport_fee FLOAT64
);