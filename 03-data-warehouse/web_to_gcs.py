# %%
# import io
import os
import requests
# import pandas as pd
from google.cloud import storage


#  https://stackoverflow.com/questions/37003862/how-to-upload-a-file-to-google-cloud-storage-on-python-3
# """
# Pre-reqs:
# 1. `pip install pandas pyarrow google-cloud-storage`
# 2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
# 3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
# """

# services = ['fhv','green','yellow']
INIT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "terraform-demo-412002-cbcccc3c0b05.json"

os.environ["GCP_GCS_BUCKET"] = "terraform-demo-412002-terra-bucket"
# switch out the bucketname
bucket = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
# %%
# storage_client = storage.Client
# %%


def upload_to_gcs(bucket_name, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    # client = storage.Client()
    client = storage.Client.from_service_account_json(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    )
    bucket = client.bucket(bucket_name)

    if storage.Blob(bucket=bucket, name=object_name).exists(client):
        print(f"GCS: {object_name} already exists")
        return
    blob = bucket.blob(object_name)
    
    blob.chunk_size = 5 * 1024 * 1024  # Set 5 MB blob size
    blob.upload_from_filename(local_file, timeout=500)
    print(f"Loaded GCS: {object_name}")


# %%
def web_to_gcs(year, service):

    for m in range(1, 13):
        # sets the month part of the file_name string
        month = f"{m:02}"

        # csv file_name
        request_url = INIT_URL.format(service=service, year=year, month=month)
        file_name = request_url.split("/")[-1]
        local_file_name = os.path.join("data", file_name)
        # print(file_name)
        # # download it using requests
        if not os.path.isfile(local_file_name):
            print(f"Getting {request_url}")
            r = requests.get(request_url, timeout=60)
            open(local_file_name, "wb").write(r.content)
            print(f"Downloaded to {local_file_name}")
        else:
            print(f"Local file {local_file_name} already exists")

        # upload it to gcs
        # gcs_object_name = f"{service}/{year}/{file_name}"
        # upload_to_gcs(bucket, gcs_object_name, local_file_name)


# %%
# df= pd.read_parquet(local_file_name, engine="pyarrow")
# df.head()
# %%
# web_to_gcs("2019", "green")
# web_to_gcs("2020", "green")
# web_to_gcs("2021", "green")
# web_to_gcs("2019", "yellow")
web_to_gcs("2020", "yellow")
# web_to_gcs("2021", "yellow")

# %%
web_to_gcs("2019", "fhv")
# %%
# import pandas as pd
import os
import duckdb
# from io import BytesIO
# %%
# for m in range(1, 13):
#         # sets the month part of the file_name string
#     month = f"{m:02}"
#     pq_file = os.path.join("data",f"fhv_tripdata_2019-{month}.parquet")
#     df = pd.read_parquet(pq_file, dtype_backend='numpy_nullable' )
#     print(f'Read {pq_file=} size:{os.stat(pq_file).st_size} records:{df.shape[0]}')
#     change_types = {"PUlocationID": "Int64", "DOlocationID": "Int64", "SR_Flag": "Int64"}
#     df = df.astype(change_types)
#     df.to_parquet(pq_file, index=False, compression='gzip')
#     print(f'Wrote to {pq_file=} size:{os.stat(pq_file).st_size}')
# %%
# service ='green'
# year = 2020
# m = 1
for service in ['yellow', 'green']:
    for year in [2020,2021]:
        for m in range(1, 13):
            month = f"{m:02}"
            pq_file = os.path.join("data",f"{service}_tripdata_{year}-{month}.parquet")
            csv_file = pq_file.replace(".parquet", '.csv.gz')
            # df = pd.read_parquet(pq_file)
            ttable = duckdb.read_parquet(pq_file)
            count_ = duckdb.sql("SELECT count(*) FROM ttable" ).df().loc[0,'count_star()']
            # print(f'Read {pq_file=} size:{os.stat(pq_file).st_size} records:{df.shape[0]}')
            print(f'Read     {pq_file=} size:{os.stat(pq_file).st_size:>12} records:{count_:>10}')
            # df.to_csv(csv_file, index=False, compression='gzip')
            ttable.write_csv(csv_file, header=True, compression='gzip')
            print(f'Wrote to {csv_file=} size:{os.stat(csv_file).st_size:>12}')
# %%
df.info()
# %%

# # %%
# csv_data = BytesIO(b'col1,col2\n1,2\n3,4')
# duckdb.read_csv(csv_data, header=True).write_parquet('csv_data.parquet')
# # %%
# pq_file = r"data\yellow_tripdata_2019-12.parquet"
# csv_file = pq_file.replace(".parquet", '.csv.gz')
# duckdb.read_parquet(pq_file).write_csv(csv_file, header=True, compression='gzip')
# # %%
# duckdb.sql("SELECT  count(*) FROM y1912 limit 10" ).df()
# # %%
# print(list(zip(y1912.columns,y1912.types)))
# # %%
# # count_ = y1912.aggregate( "count(*)")
# count_ = duckdb.sql("SELECT count(*) FROM y1912" ).df().loc[0,'count_star()']
# count_
# %%
