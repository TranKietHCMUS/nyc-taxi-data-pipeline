from datetime import datetime
import os
import pandas as pd
from deltalake import write_deltalake
from tasks.helper import load_cfg

CFG_FILE = "/opt/airflow/utils/config.yaml"

cfg = load_cfg(CFG_FILE)
data_cfg = cfg["nyc_taxi_data"]
delta_cfg = cfg["delta_table"]
datalake_cfg = cfg["datalake"]

year = str(datetime.now().year)

def convert_to_delta():
    for i in range(1, 13):
        month = str(i).zfill(2)

        file_path = f'{data_cfg["folder_path"]}/{year}/{month}/{data_cfg["data_type"]}.parquet'
        delta_folder_path = f'{delta_cfg["folder_path"]}/{year}/{month}'

        if os.path.exists(delta_folder_path):
            continue

        if os.path.exists(file_path):
            df = pd.read_parquet(file_path, engine='pyarrow')
            write_deltalake(delta_folder_path, df, mode="append")