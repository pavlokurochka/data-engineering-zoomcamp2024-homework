select
	zz."Borough" ,
 	sum(total_amount ) total_amount 
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
	 green_taxi_trips tt
	join zones zz on tt."PULocationID" = zz."LocationID" 
	where lpep_pickup_datetime::date = '2019-09-18' 
group by
	1
order by
	2 desc ;