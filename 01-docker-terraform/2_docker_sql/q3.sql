select
	lpep_pickup_datetime::date pickup,
	lpep_dropoff_datetime::date dropoff,
	  date_part('month', lpep_pickup_datetime) month_,
	count(*) count_
	--	"VendorID",
	--	lpep_pickup_datetime,
	--	lpep_dropoff_datetime,
	--	store_and_fwd_flag,
	--	"RatecodeID",
	--	"PULocationID",
	--	"DOLocationID",
	--	passenger_count,
	--	trip_distance,
	--	fare_amount,
	--	extra,
	--	mta_tax,
	--	tip_amount,
	--	tolls_amount,
	--	ehail_fee,
	--	improvement_surcharge,
	--	total_amount,
	--	payment_type,
	--	trip_type,
	--	congestion_surcharge
from
	public.green_taxi_trips
group by
	1,
	2,3
order by
	1,
	2;