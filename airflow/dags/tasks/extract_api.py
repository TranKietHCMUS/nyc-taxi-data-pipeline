from datetime import datetime
from tasks.helper import load_cfg, download_file
from minio import Minio

CFG_FILE = "/opt/airflow/utils/config.yaml"

cfg = load_cfg(CFG_FILE)
data_cfg = cfg["nyc_taxi_data"]
datalake_cfg = cfg["datalake"]


def extract_api():
    bucket_name = datalake_cfg["bucket_name1"]

    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )

    year = str(datetime.now().year)
    month = str(datetime.now().month).zfill(2)
    object_name = datalake_cfg["api_folder_name"] + "/" + year + "/" + data_cfg["data_type"] + "-" + month + ".parquet"
    download_file(minio_client, str(int(year) - 1), month, bucket_name, object_name, data_cfg["url_prefix"], data_cfg["data_type"])