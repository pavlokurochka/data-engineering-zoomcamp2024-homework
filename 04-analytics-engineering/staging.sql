CREATE
OR REPLACE EXTERNAL TABLE `ny_taxi.green_tripdata` OPTIONS (
    format = 'parquet',
    uris = ['gs://terraform-demo-412002-terra-bucket/green/2019/green_tripdata_2019-*.parquet', 'gs://terraform-demo-412002-terra-bucket/green/2020/green_tripdata_2020-*.parquet']
);

CREATE
OR REPLACE EXTERNAL TABLE `ny_taxi.yellow_tripdata` OPTIONS (
    format = 'parquet',
    uris = ['gs://terraform-demo-412002-terra-bucket/yellow/2019/yellow_tripdata_2019-*.parquet', 'gs://terraform-demo-412002-terra-bucket/yellow/2020/yellow_tripdata_2020-*.parquet']
);

CREATE
OR REPLACE EXTERNAL TABLE `ny_taxi.fhv_tripdata` OPTIONS (
    format = 'parquet',
    uris = ['gs://terraform-demo-412002-terra-bucket/fhv/2019/fhv_tripdata_2019-*.parquet' ]
);