from io import BytesIO
import pandas as pd
import requests
import yaml
from minio.error import S3Error

def load_cfg(cfg_file):
    """
    Load configuration from a YAML config file
    """
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

    return cfg


def file_exists(minio_client, bucket_name, object_name):
    try:
        minio_client.stat_object(bucket_name, object_name)
        return True  # File tồn tại
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False  # File không tồn tại
        print(f"Lỗi khác: {e}")
        return False


def download_file(minio_client, year, month, bucket_name, object_name, url_prefix, data_type):
    try:
        if file_exists(minio_client, bucket_name, object_name):
            print(f"File {object_name} has already exists in {bucket_name}.")
            return
        # Tải nội dung từ URL
        url_download = url_prefix + data_type + "_" + year + "-" + month + ".parquet"
        response = requests.get(url_download, stream=True)
        response.raise_for_status()
        
        # Upload trực tiếp nội dung lên MinIO
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=response.raw,
            length=int(response.headers.get('content-length', 0)),  # Kích thước file
            content_type=response.headers.get('content-type', 'application/octet-stream')  # Loại MIME
        )
        print(f"Successfully uploaded {object_name} to MinIO bucket {bucket_name}")
    except S3Error as e:
        print(f"MinIO Error: {e}")
    except requests.RequestException as e:
        print(f"Download file error: {e}")

def read_parquet_from_minio(minio_client, bucket_name, object_name):
    try:
        data = minio_client.get_object(bucket_name, object_name)
        df = pd.read_parquet(BytesIO(data.read()))
        print(f"Read file {object_name}: {len(df)} rows")
        return df
    except Exception as e:
        print(f"File not found: {object_name}")
        return None