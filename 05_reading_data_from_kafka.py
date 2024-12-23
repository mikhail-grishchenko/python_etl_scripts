from itertools import chain
import pandas as pd  
import numpy as np 
import time  
from dateutil import parser

from kafka import KafkaConsumer, TopicPartition  # импортируем KafkaConsumer для чтения данных из Kafka
import json  
from json import loads, dumps  

from google.cloud import bigquery  # библиотека для работы с Google BigQuery
from google.oauth2 import service_account  # библиотека для аутентификации с использованием учетных данных службы
from google.cloud.bigquery import SchemaField
import pandas_gbq as pgbq
from tqdm import tqdm

import os  # библиотека для работы с операционной системой
from datetime import datetime, date, timedelta
from json import JSONDecodeError
import gc


def proxy_decorator(func):
  def wrapper(*args, **kwargs):
    # Сохранение текущих переменных окружения для прокси
    old_http_proxy = os.environ.get('http_proxy')
    old_https_proxy = os.environ.get('https_proxy')

    # Установка прокси только внутри этой функции
    os.environ['http_proxy'] = proxies['http']
    os.environ['https_proxy'] = proxies['https']

    try:
      result = func(*args, **kwargs)
    except Exception as e:
      print(f'Ошибка в декораторе: {e}')
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
    return result
  return wrapper

    
#ФУНКЦИЯ ЗАГРУЗКИ В GBQ
@proxy_decorator    
def process_and_load(df, client):

    if df.shape[0]>0:
        # Convert the data types in the DataFrame
        # df['created_at'] = pd.to_datetime(df['created_at'])  # Convert created_at to datetime

        # Parse datetime strings and convert to local time
        df['created_at'] = pd.to_datetime(df['created_at'].apply(lambda x: parser.parse(x))).dt.tz_localize(None)# для Airlow
        
        df['insert_dt'] = pd.to_datetime(datetime.now().replace(microsecond=0))    # Convert insert_dt to datetime

        # Ensure all object columns are converted to string
        object_columns = df.select_dtypes(include=['object']).columns
        df[object_columns] = df[object_columns].astype(str)

        # Преобразование столбца created_at в datetime и извлечение только даты
        df['created_date'] = pd.to_datetime(df['created_at']).dt.date

        # Вставка нового столбца в начало DataFrame
        df.insert(0, 'created_date', df.pop('created_date'))
        
        # Группировка по partition и вычисление минимальных и максимальных значений offset
        partition_offsets = df.groupby('partition')['offset'].agg(
            min_offset='min',
            max_offset='max'
        ).reset_index()

        # Печать таблицы с минимальными и максимальными значениями offset для каждой партиции
        print(partition_offsets)
    
        # Дополнительные вычисления
        min_offset = df['offset'].min()
        max_offset = df['offset'].max()
        unique_partitions = df['partition'].unique()
        min_timestamp = df['timestamp'].min()
        max_timestamp = df['timestamp'].max()
        unique_campaign_ids = df['campaign_id'].nunique()
        unique_command_ids = df['command_id'].nunique()
        unique_customer_uuids = df['customer_uuid'].nunique()

        # Печать дополнительной информации
        print(f"df.shape - {df.shape}")
        print(f"Min offset: {min_offset}, Max offset: {max_offset}")
        print(f"Unique partitions: {unique_partitions}")
        print(f"Min timestamp: {min_timestamp}, Max timestamp: {max_timestamp}")
        print(f"Unique campaign IDs: {unique_campaign_ids}")
        print(f"Unique command IDs: {unique_command_ids}")
        print(f"Unique customer UUIDs: {unique_customer_uuids}")

        # Определяем схему для таблицы в BigQuery
        schema = [
            SchemaField('created_date', 'DATE'),
            SchemaField('topic', 'STRING'),
            SchemaField('partition', 'INT64'),
            ...
            SchemaField('insert_dt', 'DATETIME')
        ]

        # Определяем идентификаторы набора данных и таблицы в BigQuery
        dataset_id = 'adhoc'
        table_id = 'kafka_campaigns'
        
        # Загрузка данных в BigQuery
        job = client.load_table_from_dataframe(df, f'{dataset_id}.{table_id}', job_config=bigquery.LoadJobConfig(schema=schema))
        print(job.result())
        print(job.state) 
  

if __name__ == "__main__":  

    initial_path = '/data/'

    # Задаём прокси
    path_server_configs = f'{initial_path}airflow/dags/configs/'
    with open(f'{path_server_configs}proxies.json', 'r') as f:
        proxies = json.load(f)

    # Определяем путь к кредам на сервере
    path_server = f'{initial_path}airflow/dags/credentials/'

    # Загружаем учетные данные для Google BigQuery
    credentials = service_account.Credentials.from_service_account_file(f'{path_server}cred.json')
    project_id = 'project'  # идентификатор проекта в Google Cloud
    client = bigquery.Client(credentials=credentials, project=project_id)  # создаем клиент для BigQuery

    # Настраиваем потребителя Kafka
    consumer = KafkaConsumer(
        bootstrap_servers=[  # список серверов Kafka
            'dwh-kafka.msk.local:9092', 
            'dwh-kafka.msk.local:9092', 
            'dwh-kafka.msk.local:9092', 
            'dwh-kafka.msk.local:9092', 
            'dwh-kafka.msk.local:9092'
        ],
        group_id="mgr_an",  # идентификатор группы потребителей
        enable_auto_commit=True,  #  автоматическое подтверждение смещений
        value_deserializer=lambda x: loads(x.decode('utf-8')),  # десериализация значений сообщений из JSON
        fetch_max_bytes=262144000,  # максимальный размер данных для выборки (в байтах)
        max_partition_fetch_bytes=5242880,  # максимальный размер данных для выборки из одного раздела (в байтах)
        max_poll_records=10000,  # максимальное количество сообщений для выборки за один раз
    #     max_poll_records=10,  # максимальное количество сообщений для выборки за один раз
        auto_offset_reset='earliest'  # начало чтения сообщений с самого начала (альтернативный вариант закомментирован ниже)
        # auto_offset_reset='latest'  # начало чтения с последнего сообщения
    )

    # Подписываемся на топик Kafka 'promo.competitor_prices'
    consumer.subscribe(['cdp_campaigns'])


    result_df = pd.DataFrame()
    counter = 0
    
    empty_msg_counter = 0
    max_empty_messages = 20  # Максимальное количество пустых сообщений
    start = time.time()  # фиксируем время начала обработки сообщений

    # Запускаем цикл для чтения сообщений из Kafka
    try:
        while empty_msg_counter <= max_empty_messages:
            # Получаем сообщения из Kafka с таймаутом 10 секунд
            msg = consumer.poll(timeout_ms=10000)
            if msg:  # если сообщения получены
                empty_msg_counter = 0
                # start = time.time()  # фиксируем время начала обработки сообщений
                # print(datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' - Got a message from ' + str(len(msg)) + ' part. ')

                # Объединяем все ConsumerRecord в один плоский список
                all_records = list(chain.from_iterable(msg.values()))

                # Создание списка словарей: Использование генератора списка для создания списка словарей, 
                # объединяя служебные поля и значения из value
                records_as_dicts = [
                    {
                        **record._asdict(),  # Преобразование namedtuple в словарь
                        **record.value       # Добавление значений из вложенного словаря value
                    }
                    for record in all_records
                ]

                # Преобразование списка словарей в DataFrame
                current_df = pd.DataFrame(records_as_dicts)
                # Удаление столбца value
                current_df.drop(columns=['value'], inplace=True)

                result_df = pd.concat([result_df, current_df], axis=0)

                # end = round(time.time() - start, 1)  # фиксируем время окончания обработки сообщений
                # print(f"{datetime.today().strftime('%Y-%m-%d %H:%M:%S')} - proc.time {str(end)} sec,  {str(len(all_records))} messages total, {str(result_df.shape)} rows \n")

                counter +=1
                if counter >= 100:
                    process_and_load(result_df, client)
                    counter = 0
                    result_df = pd.DataFrame()
                    end = round(time.time() - start, 1)  # фиксируем время окончания обработки сообщений
                    print(f"{datetime.today().strftime('%Y-%m-%d %H:%M:%S')} - proc.time {str(end)} sec \n")
                    start = time.time()  # фиксируем следующее время начала обработки сообщений

            else:
                if empty_msg_counter == max_empty_messages:
                    process_and_load(result_df, client)
                empty_msg_counter += 1
                print(datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' Waiting for 30 sec.\n')
                time.sleep(30)
                continue  # продолжаем цикл
            
    except KeyboardInterrupt:
        print("Stopped by user")       

    finally:
        consumer.close() 


    try:
        del result_df
        del current_df
        del partition_offsets
        del all_records
        del records_as_dicts
        del counter
        del empty_msg_counter
        del consumer
        gc.collect()
    except Exception as ex:
        print(ex)
