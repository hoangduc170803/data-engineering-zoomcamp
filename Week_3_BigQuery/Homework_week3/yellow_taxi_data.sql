

#Create External_Table
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://yellow_ny_taxi_2024/raw/output_yellow_taxi_2024*.parquet']
);


SELECT COUNT(1) FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024`;
SELECT * FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024` LIMIT 10;
--20332093

---------------------------------------------------------------------------------------------------------------------
--Create Materialized_Table
CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.materialized_yellow_taxi_data_2024`
AS
SELECT *
FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024`;

-- External_Table
SELECT DISTINCT PULocationID FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024`;

# Materialized_Table
SELECT DISTINCT PULocationID FROM `terraform-demo-447612.taxi_data.materialized_yellow_taxi_data_2024`;



--0 MB for the External Table and 155.12 MB for the Materialized Table
----------------------------------------------------------------------------------------------------------------------------------------------------

--155.12 MB for PULocationID
SELECT PULocationID FROM `terraform-demo-447612.taxi_data.materialized_yellow_taxi_data_2024`;

--310.24 MB for PULocationID,DOLocationID
SELECT PULocationID,DOLocationID FROM `terraform-demo-447612.taxi_data.materialized_yellow_taxi_data_2024`;



--BigQuery is a columnar database, and it only scans the specific columns requested in the query.
--Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

----------------------------------------------------------------------------------------------------------------------------------------------------

SELECT COUNT(fare_amount)  FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024` WHERE fare_amount = 0;
--8,333

-----------------------------------------------------------------------------------------------------------------------------------------------------
--Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT *
FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024`;

------------------------------------------------------------------------------------------------------------------------------------------------------
--310.24 MB
SELECT distinct VendorID FROM `terraform-demo-447612.taxi_data.materialized_yellow_taxi_data_2024` WHERE  DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';

--26.84 MB
SELECT distinct VendorID FROM `terraform-demo-447612.taxi_data.external_yellow_taxi_data_2024_partitioned_clustered` WHERE  DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';

--310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


-----------------------------------------------------------------------------------------------------------------------------------------------------

--GCP Bucket


--------------------------------------------------------------------------------------------------------------------------------------------------

--False

--------------------------------------------------------------------------------------------------------------------------------------------------

--0 B 