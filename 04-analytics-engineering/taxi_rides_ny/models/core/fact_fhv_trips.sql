{{ config(materialized="table") }}

with dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    dispatching_base_num,
    affiliated_base_number,
    pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    -- timestamps
    pickup_datetime,
    dropoff_datetime,
    -- trip info
    shared_ride_flag
from {{ ref("stg_fhv_tripdata") }} as trips
inner join
    dim_zones as pickup_zone on trips.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on trips.dropoff_locationid = dropoff_zone.locationid
