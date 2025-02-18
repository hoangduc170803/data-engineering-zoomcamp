#Create External_Table FHV
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-447612.taxi_data.external_fhv_data_2019`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_taxi_data__parquet_bucket/raw/output_fhv_trip_2019-*.parquet']
);

CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.fhv_data_2019`
AS
SELECT *
FROM `terraform-demo-447612.taxi_data.external_fhv_data_2019`;

#Create External_Table GreenTaxi 2019-2020
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-447612.taxi_data.external_green_taxi_2019_2020`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_taxi_data__parquet_bucket/raw/output_green_taxi_2019-*.parquet','gs://nyc_taxi_data__parquet_bucket/raw/output_green_taxi_2020-*.parquet']
);

CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.green_taxi_2019_2020`
AS
SELECT *
FROM `terraform-demo-447612.taxi_data.external_green_taxi_2019_2020`;

#Create External_Table yellow_taxi_2019_2020
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2019_2020`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_taxi_data__parquet_bucket/raw/output_yellow_taxi2019-*.parquet','gs://nyc_taxi_data__parquet_bucket/raw/output_yellow_taxi2020-*.parquet']
);

CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.yellow_taxi_data_2019_2020`
AS
SELECT *
FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2019_2020`;
