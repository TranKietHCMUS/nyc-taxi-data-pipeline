from datetime import datetime
from tasks.helper import load_cfg
from minio import Minio
from minio.error import S3Error
from sqlalchemy import create_engine
import io
import pandas as pd

CFG_FILE = "/opt/airflow/utils/config.yaml"

cfg = load_cfg(CFG_FILE)
data_cfg = cfg["nyc_taxi_data"]
datalake_cfg = cfg["datalake"]
mysql_cfg = cfg["mysql"]

# read data from mysql
def get_data_from_db(minio_client, year, month, bucket_name, object_name):
    try:
        connection_string = f"mysql+pymysql://{mysql_cfg['user']}:{mysql_cfg['password']}@{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}"
        
        engine = create_engine(connection_string)
        
        query = f"""
            SELECT *
            FROM {mysql_cfg['table']}
            WHERE YEAR(updated_at) = {year}
            AND MONTH(updated_at) = {month}
        """
        
        df = pd.read_sql(query, engine)

        if df.empty:
            print(f"No data available for {month}/{year}")
            return
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)
        
        parquet_buffer.seek(0)
        
        # Upload file lên MinIO
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/parquet'
        )
        
        print(f"Successfully uploaded {object_name} to MinIO bucket {bucket_name}")
    except S3Error as e:
        print(f"MinIO Error: {e}")
        return False
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e


def extract_db():
    bucket_name = datalake_cfg["bucket_name1"]

    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )
    
    year = str(datetime.now().year)
    month = str(datetime.now().month).zfill(2)

    object_name = datalake_cfg["db_folder_name"] + "/" + year + "/" + data_cfg["data_type"] + "-" + month + ".parquet"
    get_data_from_db(minio_client, year, month, bucket_name, object_name)