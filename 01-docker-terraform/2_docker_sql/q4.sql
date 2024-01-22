select
	lpep_pickup_datetime::date pickup,
 	max(trip_distance) longest
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
	1
order by
	2 desc ;