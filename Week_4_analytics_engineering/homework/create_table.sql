
#Create External_Table
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-447612.taxi_data.external_fhv_data_2019`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_taxi_data__parquet_bucket/raw/output_fhv_trip_2019-*.parquet']
);

CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.fhv_data_2019`
AS
SELECT *
FROM `terraform-demo-447612.taxi_data.external_fhv_data_2019`;
