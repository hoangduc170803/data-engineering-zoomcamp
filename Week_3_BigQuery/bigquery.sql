
CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.yellow_tripdata_non_partitioned` AS 
SELECT * FROM `terraform-demo-447612.taxi_data.external_yellow_tripdata`;



CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.yellow_tripdata_partitioned` PARTITION BY DATE(tpep_pickup_datetime) AS SELECT * FROM `terraform-demo-447612.taxi_data.external_yellow_tripdata`;


SELECT DISTINCT(VendorID) FROM `terraform-demo-447612.taxi_data.yellow_tripdata_non_partitioned` WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT DISTINCT(VendorID) FROM `terraform-demo-447612.taxi_data.yellow_tripdata_partitioned` WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT table_name, partition_id, total_rows
FROM `taxi_data.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

CREATE OR REPLACE TABLE `terraform-demo-447612.taxi_data.yellow_tripdata_partitioned_clustered` PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY (VendorID)
 AS SELECT * FROM `terraform-demo-447612.taxi_data.external_yellow_tripdata`;

SELECT count(*) AS Trip FROM `terraform-demo-447612.taxi_data.yellow_tripdata_partitioned` WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31' AND VendorID = 1;

SELECT count(*) AS Trip FROM `terraform-demo-447612.taxi_data.yellow_tripdata_partitioned_clustered` WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
AND VendorID = 1;
