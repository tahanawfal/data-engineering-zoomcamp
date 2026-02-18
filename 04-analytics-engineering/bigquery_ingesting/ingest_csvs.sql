-- creates external tables in BigQuery that reference yellow CSV files stored in a GS bucket.
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-project-485916.dbt_tahanawfal.external_yellow_taxi`
OPTIONS (
  format = 'csv',
  uris = ['gs://kestra-zoomcamp-taha-demo/yellow_tripdata_*.csv.gz']
);

-- creates external tables in BigQuery that reference green CSV files stored in a GS bucket.
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-project-485916.dbt_tahanawfal.external_green_taxi`
OPTIONS (
  format = 'csv',
  uris = ['gs://kestra-zoomcamp-taha-demo/green_tripdata_*.csv.gz']
);

-- creates external tables in BigQuery that reference fhv CSV files stored in a GS bucket.
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-project-485916.dbt_tahanawfal.external_fhv_taxi`
OPTIONS (
  format = 'csv',
  uris = ['gs://kestra-zoomcamp-taha-demo/fhv_tripdata_*.csv.gz']
);

-- creates internal green tables in BigQuery from the external tables.
CREATE OR REPLACE TABLE `zoomcamp-project-485916.dbt_tahanawfal.green_taxi_trips` AS
SELECT * FROM `zoomcamp-project-485916.dbt_tahanawfal.external_green_taxi`;

-- creates internal fhv tables in BigQuery from the external tables.
CREATE OR REPLACE TABLE `zoomcamp-project-485916.dbt_tahanawfal.fhv_taxi_trips` AS
SELECT * FROM `zoomcamp-project-485916.dbt_tahanawfal.external_fhv_taxi`;

-- creates internal yellow tables in BigQuery from the external tables.
CREATE OR REPLACE TABLE `zoomcamp-project-485916.dbt_tahanawfal.yellow_taxi_trips` AS
SELECT * FROM `zoomcamp-project-485916.dbt_tahanawfal.external_yellow_taxi`;