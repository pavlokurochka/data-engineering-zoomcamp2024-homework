-- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/03-data-warehouse/homework.md
-- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/big_query.sql
-- Creating external table referring to gcs path
CREATE
OR REPLACE EXTERNAL TABLE `ny_taxi.external_green_tripdata` OPTIONS (
  format = 'parquet',
  uris = ['gs://terraform-demo-412002-terra-bucket/green/2022/green_tripdata_2022-*.parquet']
);

-- Create a non partitioned table from external table
CREATE
OR REPLACE TABLE ny_taxi.green_tripdata_non_partitoned AS
SELECT
  *
FROM
  `ny_taxi.external_green_tripdata`;

-- Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT
  count(1)
from
  `ny_taxi.external_green_tripdata`;

-- 840402
-- Question 2:
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT
  count(DISTINCT PULocationID)
from
  `ny_taxi.external_green_tripdata`;

-- This query will process 0 B when run.
SELECT
  count(DISTINCT PULocationID)
from
  `ny_taxi.green_tripdata_non_partitoned`;

--This query will process 6.41 MB when run
-- Question 3: How many records have a fare_amount of 0?
SELECT
  count(1)
from
  `ny_taxi.external_green_tripdata`
where
  fare_amount = 0;

-- 1622
-- Question 4:
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results 
-- by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
--partition by lpep_pickup_datetime and cluster on PUlocationID.
-- Creating a partition and cluster table
CREATE
OR REPLACE TABLE ny_taxi.green_tripdata_partitioned_clustered PARTITION BY DATE(lpep_pickup_datetime) CLUSTER BY PUlocationID AS
SELECT
  *
FROM
  ny_taxi.external_green_tripdata;

--   Question 5:
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
-- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

SELECT
  DISTINCT PULocationID
FROM
  ny_taxi.green_tripdata_partitioned_clustered
WHERE
  TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY)>= TIMESTAMP('2022-06-01')
  AND TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) <= TIMESTAMP('2022-06-30');

-- Bytes processed 1.12 MB
SELECT
  DISTINCT PULocationID
FROM
  ny_taxi.green_tripdata_non_partitoned
WHERE
  TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY)>= TIMESTAMP('2022-06-01')
  AND TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) <= TIMESTAMP('2022-06-30'); 
  -- Bytes processed 12.82 MB