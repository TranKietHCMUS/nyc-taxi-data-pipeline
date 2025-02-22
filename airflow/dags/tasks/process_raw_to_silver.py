import pandas as pd
from minio import Minio
from io import BytesIO
from datetime import datetime
from tasks.helper import load_cfg, read_parquet_from_minio

CFG_FILE = "/opt/airflow/utils/config.yaml"

cfg = load_cfg(CFG_FILE)
data_cfg = cfg["nyc_taxi_data"]
datalake_cfg = cfg["datalake"]

def merge_and_clean_monthly_data(minio_client, source_bucket, des_bucket, year, month, data_type):
    try:
        api_object_name = f"{datalake_cfg['api_folder_name']}/{year}/{data_type}-{month}.parquet"
        db_object_name = f"{datalake_cfg['db_folder_name']}/{year}/{data_type}-{month}.parquet"
        merged_object_name = f"{year}/{data_type}-{month}.parquet"
        
        # Read data from both sources
        api_df = read_parquet_from_minio(minio_client, source_bucket, api_object_name)
        db_df = read_parquet_from_minio(minio_client, source_bucket, db_object_name)
        
        # Handle different data availability scenarios
        if api_df is None and db_df is None:
            print(f"No data available for {month}/{year}")
            return
        
        if api_df is None:
            print("Only database data available")
            db_df = db_df.drop(columns=["id"])
            merged_df = db_df
        elif db_df is None:
            print("Only API data available")
            api_df = api_df.drop(columns=["store_and_fwd_flag"])
            api_df["tpep_pickup_datetime"] = api_df["tpep_pickup_datetime"].astype('datetime64[s]')
            api_df["tpep_dropoff_datetime"] = api_df["tpep_dropoff_datetime"].astype('datetime64[s]')

            api_df = api_df.rename(columns={
                "VendorID": "vendor_id",
                "RatecodeID": "rate_code_id",
                "PULocationID": "pu_location_id",
                "DOLocationID": "do_location_id",
                "Airport_fee": "airport_fee",
            })

            api_df["vendor_id"] = api_df["vendor_id"].astype('int64')
            api_df["pu_location_id"] = api_df["pu_location_id"].astype('int64')
            api_df["do_location_id"] = api_df["do_location_id"].astype('int64')
            api_df["created_at"] = datetime.now()
            api_df["updated_at"] = datetime.now()

            merged_df = api_df
        else:
            print("Merging data from both sources")
            # Drop columns from db data before merging
            db_df = db_df.drop(columns=["id"])

            api_df = api_df.drop(columns=["store_and_fwd_flag"])
            api_df["tpep_pickup_datetime"] = api_df["tpep_pickup_datetime"].astype('datetime64[s]')
            api_df["tpep_dropoff_datetime"] = api_df["tpep_dropoff_datetime"].astype('datetime64[s]')

            api_df = api_df.rename(columns={
                "VendorID": "vendor_id",
                "RatecodeID": "rate_code_id",
                "PULocationID": "pu_location_id",
                "DOLocationID": "do_location_id",
                "Airport_fee": "airport_fee",
            })

            api_df["vendor_id"] = api_df["vendor_id"].astype('int64')
            api_df["pu_location_id"] = api_df["pu_location_id"].astype('int64')
            api_df["do_location_id"] = api_df["do_location_id"].astype('int64')
            api_df["created_at"] = datetime.now()
            api_df["updated_at"] = datetime.now()

            # Merge dataframes
            merged_df = pd.concat([api_df, db_df], ignore_index=True).drop_duplicates()

        # add new columns "id" with an auto-incremental value
        merged_df["id"] = range(1, len(merged_df) + 1)

        merged_df["created_at"] = merged_df["created_at"].astype('datetime64[s]')
        merged_df["updated_at"] = merged_df["updated_at"].astype('datetime64[s]')

        # drop rows with trip_distance < 0 or fare_amount < 0 or total_amount < 0
        merged_df = merged_df[merged_df["trip_distance"] >= 0]
        merged_df = merged_df[merged_df["fare_amount"] >= 0]
        merged_df = merged_df[merged_df["total_amount"] >= 0]

        print(merged_df.dtypes)
        
        # Convert DataFrame to parquet
        parquet_buffer = BytesIO()
        merged_df.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        # Upload merged file to MinIO
        minio_client.put_object(
            bucket_name=des_bucket,
            object_name=merged_object_name,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/parquet'
        )
        
        print(f"Successfully uploaded merged file to bucket {des_bucket}: {merged_object_name}")
        
    except Exception as e:
        print(f"Error during data merge: {str(e)}")
        raise e

def process_raw_to_silver():
    # Initialize MinIO client
    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False
    )
    
    bucket_name1 = datalake_cfg["bucket_name1"]
    bucket_name2 = datalake_cfg["bucket_name2"]
    
    # Get current year and month
    year = str(datetime.now().year)
    month = str(datetime.now().month).zfill(2)
    
    # Execute merge
    merge_and_clean_monthly_data(
        minio_client=minio_client,
        source_bucket=bucket_name1,
        des_bucket=bucket_name2,
        year=year,
        month=month,
        data_type=data_cfg["data_type"]
    )