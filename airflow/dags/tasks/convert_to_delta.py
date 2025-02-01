import pandas as pd
import pyarrow as pa
from minio import Minio
from datetime import datetime
from tasks.helper import load_cfg, read_parquet_from_minio
from deltalake import DeltaTable, write_deltalake

# Load configuration
CFG_FILE = "/opt/airflow/utils/config.yaml"
cfg = load_cfg(CFG_FILE)
data_cfg = cfg["nyc_taxi_data"]
datalake_cfg = cfg["datalake"]

def merge_into_delta_table(minio_client, source_bucket, des_bucket, year, month, data_type):
    try:
        # Đường dẫn đến Delta Table đích trên MinIO
        delta_table_uri = f"s3://{des_bucket}/{data_type}"

        # Cấu hình storage options cho delta table
        storage_options = {
            "AWS_ACCESS_KEY_ID": datalake_cfg["access_key"],
            "AWS_SECRET_ACCESS_KEY": datalake_cfg["secret_key"],
            "AWS_ENDPOINT_URL": f"http://{datalake_cfg['endpoint']}",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        # Đọc dữ liệu mới từ source bucket
        source_path = f"{year}/{data_type}-{month}.parquet"
        new_df = read_parquet_from_minio(minio_client, source_bucket, source_path)
        
        if new_df is None or new_df.empty:
            print("No valid data found to merge")
            return

        try:
            # Thử đọc Delta Table hiện có
            dt = DeltaTable(delta_table_uri, storage_options=storage_options)
            existing_df = dt.to_pandas()
            print("Successfully read existing Delta Table")

            # Merge dữ liệu
            merged_df = pd.concat([
                existing_df[~existing_df.set_index(['tpep_pickup_datetime', 'tpep_dropoff_datetime']).index.isin(
                    new_df.set_index(['tpep_pickup_datetime', 'tpep_dropoff_datetime']).index
                )],  # Giữ lại các bản ghi cũ không có trong dữ liệu mới
                new_df  # Thêm tất cả dữ liệu mới
            ]).reset_index(drop=True)

            # Ghi lại vào Delta Table
            write_deltalake(
                delta_table_uri,
                merged_df,
                mode='append',
                storage_options=storage_options
            )
            print("Successfully merged and wrote data to Delta Table")

        except Exception as e:
            if "log" in str(e).lower():
                print(f"Delta Table does not exist, creating new one")
                try:
                    # Tạo Delta Table mới với dữ liệu từ new_df
                    write_deltalake(
                        delta_table_uri,
                        new_df,
                        mode='append',
                        storage_options=storage_options
                    )
                    print("Successfully created new Delta Table")
                except Exception as write_error:
                    print(f"Error creating Delta Table: {write_error}")
                    return
            else:
                print(f"Error merge data: {e}")
                return

    except Exception as e:
        print(f"Unexpected error: {e}")
        return

def convert_to_delta():
    try:
        # Khởi tạo MinIO client
        minio_client = Minio(
            endpoint=datalake_cfg["endpoint"],
            access_key=datalake_cfg["access_key"],
            secret_key=datalake_cfg["secret_key"],
            secure=False
        )

        # Lấy tên bucket từ cấu hình
        bucket_name2 = datalake_cfg["bucket_name2"]
        bucket_name3 = datalake_cfg["bucket_name3"]

        # Lấy năm và tháng hiện tại
        year = str(datetime.now().year)
        month = str(datetime.now().month).zfill(2)

        # Gọi hàm merge dữ liệu vào Delta Table
        merge_into_delta_table(
            minio_client, 
            bucket_name2, 
            bucket_name3, 
            year, 
            month, 
            data_cfg["data_type"]
        )

    except Exception as e:
        print(f"Error in convert_to_delta: {e}")
        raise