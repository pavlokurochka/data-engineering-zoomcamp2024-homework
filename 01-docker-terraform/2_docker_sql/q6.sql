select
	zp."Zone" ,
	zd."Zone" ,
	max(tip_amount) tip_amount
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
join zones zp on
	tt."PULocationID" = zp."LocationID"
join zones zd on
	tt."DOLocationID" = zd."LocationID"
where
	date_part('month',
	lpep_pickup_datetime) = 9
	and zp."Zone" = 'Astoria'
group by
	1,
	2
order by
	3 desc ;