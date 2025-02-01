from tasks.helper import load_cfg, download_file
from minio import Minio

CFG_FILE = "/opt/airflow/utils/config.yaml"

cfg = load_cfg(CFG_FILE)
data_cfg = cfg["nyc_taxi_data"]
datalake_cfg = cfg["datalake"]

def create_bucket():
    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )
    bucket_name1 = datalake_cfg["bucket_name1"]
    found1 = minio_client.bucket_exists(bucket_name=bucket_name1)
    if not found1:
        minio_client.make_bucket(bucket_name=bucket_name1)
    else:
        print(f'Bucket {bucket_name1} already exists, skip creating!')
    
    bucket_name2 = datalake_cfg["bucket_name2"]
    found2 = minio_client.bucket_exists(bucket_name=bucket_name2)
    if not found2:
        minio_client.make_bucket(bucket_name=bucket_name2)
    else:
        print(f'Bucket {bucket_name2} already exists, skip creating!')
    
    bucket_name3 = datalake_cfg["bucket_name3"]
    found3 = minio_client.bucket_exists(bucket_name=bucket_name3)
    if not found3:
        minio_client.make_bucket(bucket_name=bucket_name3)
    else:
        print(f'Bucket {bucket_name3} already exists, skip creating!')