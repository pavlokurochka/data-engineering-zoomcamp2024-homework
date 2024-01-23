
# docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v d:/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13   

  docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4

  # in connection use host pg-database 

docker build -t taxi_ingest:v001 .

  # Run the script with Docker


# ```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
# URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
  
    # --host=pg-database \

docker run -it \
  --network=pg-network \
    --name taxi_ingest \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database  \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
# ```

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
  
    # --host=pg-database \

docker run -it \
  --network=pg-network \
    --name taxi_ingest_green \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database  \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}
# ```
URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
  
    # --host=pg-database \

docker run -it \
  --network=pg-network \
    --name taxi_ingest_zones \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database  \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zones \
    --url=${URL}

#  docker rm taxi_ingest_zones 
