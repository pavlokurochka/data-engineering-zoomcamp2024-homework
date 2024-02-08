-- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/03-data-warehouse/homework.md
-- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/big_query.sql

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://terraform-demo-412002-terra-bucket/green/2022/green_tripdata_2022-*.parquet']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM  `ny_taxi.external_green_tripdata`;

-- Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT count(1) from  `ny_taxi.external_green_tripdata`;
-- 840402

-- Question 2:
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT count(DISTINCT PULocationID) from  `ny_taxi.external_green_tripdata`;
-- This query will process 0 B when run.

SELECT count(DISTINCT PULocationID) from  `ny_taxi.green_tripdata_non_partitoned`;
--This query will process 6.41 MB when run

-- Question 3: How many records have a fare_amount of 0?
SELECT count(1) from  `ny_taxi.external_green_tripdata` where fare_amount =0;
-- 1622