import pandas as pd
import boto3
from botocore.client import Config
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, date, timedelta
from pandas_gbq import gbq
import tqdm
import gc

import os
import sys
import fcntl
import pwd
import time
import json


# блокировка параллельных запусков 
def prevent_duplicates(lock_file):
    # Попытка открыть файл с блокировкой
    try:
        file_descriptor = os.open(lock_file, os.O_CREAT | os.O_RDWR)
        fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print("Скрипт запущен")

        #Получение PID текущего процесса
        pid = os.getpid()
        # Получение информации о текущем пользователе, запускающем процесс
        user = pwd.getpwuid(os.geteuid())[0]
        print(f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.PID, USER текущего процесса:{pid}, {user}")
    except OSError as e:
        print(f"{e}")
        if e.errno == 11:
            pid = os.getpid()
            user = pwd.getpwuid(os.geteuid())[0]
            print("Дубликат скрипта уже запущен")
            os._exit(1)
        else:
            raise

# Запрос к GBQ
def get_data_from_gbq(req):
    # Сохранение текущих переменных окружения для прокси
    old_http_proxy = os.environ.get('http_proxy')
    old_https_proxy = os.environ.get('https_proxy')

    # Установка прокси только внутри этой функции
    os.environ['http_proxy'] = proxies['http']
    os.environ['https_proxy'] = proxies['https']

    try:
        df = gbq.read_gbq(req, project_id=project_id, credentials=credentials)
        print(f'df.shape - {df.shape}')
        return df
    except Exception as e:
        print(f'Error while reading gbq: {e}')

    finally:
        #Восстановление оригинальных переменных окружения (если они существовали)
        if old_http_proxy is not None:
            os.environ['http_proxy'] = old_http_proxy
        else:
            os.environ.pop('http_proxy', None)  # Удаляем переменную, если ее не было

        if old_https_proxy is not None:
            os.environ['https_proxy'] = old_https_proxy
        else:
            os.environ.pop('https_proxy', None)  # Удаляем переменную, если ее не было


# Функция для загрузки DataFrame в S3 в формате Parquet
def upload_dataframe_to_s3_parquet(dataframe, s3_client, bucket_name, object_name):
    # Сохранение DataFrame в Parquet в память
    buffer = BytesIO()
    # Готовим таблицу из dataframe
    table = pa.Table.from_pandas(dataframe)
    
    pq.write_table(table, buffer)
    
    # Перемещение указателя в начало буфера
    buffer.seek(0)
    
    try:
        # Загрузка Parquet файла в S3
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=buffer.getvalue())
        print(f"DataFrame uploaded to bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
        print(f"Error uploading DataFrame: {e}")
        sys.exit(1)


# Функция для получения списка файлов в бакете
def list_files_in_bucket(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])
    else:
        print(f"No files found in bucket {bucket_name}")
        
        

# Function to create an S3 client
def create_s3_client(cred):
    bucket_name, endpoint_url, region_name, access_key, secret_access_key = cred

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=Config(signature_version='s3v4')
    )
    return s3_client, bucket_name


if __name__ == "__main__":
    # initial_path = '/data/composes/'
    initial_path = '/opt/'

    lock_path_server = f'{initial_path}airflow/dags/lockfiles/'
    path_server = f'{initial_path}airflow/dags/cred/'
    s3_path_server = f'{initial_path}airflow/dags/cred/'
    
    path_server_configs = F'{initial_path}airflow/dags/configs/'
    with open(f'{path_server_configs}proxies.json', 'r') as f:
        proxies = json.load(f)

    # Путь к файлу, который будет использоваться для блокировки
    lock_file = f'{lock_path_server}promotins.lock'
    prevent_duplicates(lock_file)


    credentials = service_account.Credentials.from_service_account_file(f'{path_server}cred.json')
    project_id = 'project'
    clientBQ = bigquery.Client(credentials=credentials,project=project_id)
    job_config = bigquery.QueryJobConfig()


    cred_test = pd.read_json(f'{initial_path}/airflow/dags/credentials/bucket_s3_cred_product.json', typ='series') 
    cred_prod = pd.read_json(f'{initial_path}/airflow/dags/credentials/bucket_s3_cred_product_category.json', typ='series')

    current_datetime = (pd.to_datetime(datetime.now())).strftime('%Y-%m-%d')

    req1 = f'''
    select * except(snap_dt)
    from `project.adv_cashflow.promotins`
    where snap_dt = current_date()
    '''

    req2 = """
    select * from `project.mon.index`
    """


    df_gbq1 = get_data_from_gbq(req1)
    df_gbq2 = get_data_from_gbq(req2)
    
     # Upload to both buckets
    for cred in [cred_test, cred_prod]:
        s3_client, bucket_name = create_s3_client(cred)

        # Вызов функции для загрузки DataFrame
        upload_dataframe_to_s3_parquet(df_gbq1, s3_client, bucket_name, f"umka/{current_datetime}/bid_budget.parquet")
        upload_dataframe_to_s3_parquet(df_gbq2, s3_client, bucket_name, f"umka/{current_datetime}/index_visible.parquet")
        
        # Вызов функции для отображения списка файлов
        list_files_in_bucket(s3_client, bucket_name)


    del df_gbq1
    del df_gbq2
    gc.collect()
