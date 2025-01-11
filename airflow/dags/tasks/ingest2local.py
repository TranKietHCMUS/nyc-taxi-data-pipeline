from datetime import datetime
import os
import requests
from tasks.helper import load_cfg

CFG_FILE = "/opt/airflow/utils/config.yaml"

cfg = load_cfg(CFG_FILE)
data_cfg = cfg["nyc_taxi_data"]
delta_cfg = cfg["delta_table"]
datalake_cfg = cfg["datalake"]

# year = str(datetime.now().year)
year = "2024"

def download_data(year, month):
    url_download = data_cfg["url_prefix"] + data_cfg["data_type"] + "_" + year + "-" + month + ".parquet"
    print(url_download)

    file_path = data_cfg["folder_path"] + "/" + year + "/" + month + "/" + data_cfg["data_type"] + ".parquet"
    os.makedirs(data_cfg["folder_path"] + "/" + year, exist_ok=True)
    os.makedirs(data_cfg["folder_path"] + "/" + year + "/" + month, exist_ok=True)

    if os.path.exists(file_path):
        print("File already exists: " + file_path)
        return False
    
    try:
        r = requests.get(url_download, allow_redirects=True)
        if (r.status_code == 200):
            open(file_path, "wb").write(r.content)
            return True
    except:
        print("Error in downloading file: " + url_download)
    return False

def ingest_to_local(**kwargs):
    ti = kwargs['ti']
    months = ti.xcom_pull(key='months', task_ids='ingest_to_local') or []

    for i in range(1, 13):
        month = str(i).zfill(2)
        
        if download_data(year, month):
            months.append(i)
    
    ti.xcom_push(key='months', value=months)