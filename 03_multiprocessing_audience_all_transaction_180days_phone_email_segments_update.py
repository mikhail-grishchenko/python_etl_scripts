import pandas_gbq as gbq
import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import multiprocessing as mp
from multiprocessing import Process, Queue, JoinableQueue, Semaphore
import pandas as pd
import os
import pwd
import random
import time
import json
import requests
import gc
import sys

path_server_configs = '/opt/airflow/dags/configs/' 
with open(f'{path_server_configs}proxies.json', 'r') as f:
        proxies = json.load(f)

# Параметры подключения к BigQuery и настройки проекта
# path_server = '/data/composes/airflow/dags/credentials/'
path_server = '/opt/airflow/dags/credentials/'
# path_server = ''

credentials = service_account.Credentials.from_service_account_file(f'{path_server}cred.json')

project_id = 'project'
clientBQ = bigquery.Client(credentials=credentials,project=project_id)
job_config = bigquery.QueryJobConfig()

ym_token = pd.read_json(f'{path_server}ya_first_transaction_token.json', typ='series') 

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
      sys.exit(1)
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


# Семафор для ожидания завершения всех процессов
lock = Semaphore(value=0)

# Создание семафора для ожидания завершения всех процессов
finished = mp.Semaphore(0)

# Функция для загрузки данных из BigQuery
@proxy_decorator
def load_data(start_date, end_date, queue):
    try:
        # Запрос данных из BigQuery
        query = f'''SELECT 
    fat.orderid,
    TO_HEX(MD5(c.phone)) as phone,
    TO_HEX(MD5(c.email)) as email,
    FROM 
    `store.ev_and_attr.final_attribution` fat
    LEFT JOIN 
    `project.dwh_input.Customer` c ON c.customer_code = fat.cardnumber
    WHERE 1=1
    AND TIMESTAMP(fat.date) >= TIMESTAMP('{start_date}') 
    AND TIMESTAMP(fat.date) < TIMESTAMP('{end_date}')
    GROUP BY 
    1, 2, 3
    '''
        print(f"Process PID: {os.getpid()} - User: {pwd.getpwuid(os.geteuid())[0]}", flush = True)

        df = gbq.read_gbq(query, project_id=project_id, credentials=credentials)
        queue.put(df)
        print(f"Добавлен еще один df в очередь queue, размер очереди: {queue.qsize()}", flush=True)
    finally:
        # Увеличиваем счетчик на 1
        with counter.get_lock():
            counter.value += 1
            print(f'counter.value - {counter.value}', flush=True)
            # Если все процессы завершены, освобождаем семафор
            if counter.value >= num_processes:
                finished.release()
    

# Функция для разделения периода на дни и создания запросов
def create_queries(start_date, end_date, num_processes):
    delta = (end_date - start_date) / num_processes
    queries = []
    for i in range(num_processes):
        query_start_date = start_date + i * delta
        query_end_date = query_start_date + delta
        queries.append((query_start_date, query_end_date))
    return queries




# ОТПРАВКА ТРАНЗАКЦИЙ 
def process_and_send_data(filtered_df):
    #ОБРАБОТКА
    # Получение размера датафрейма в байтах
    size_bytes = filtered_df.memory_usage(deep=True).sum()

    # Перевод размера из байтов в гигабайты
    size_gb = size_bytes / (1024 ** 3)

    print(f"Размер датафрейма: {size_gb:.2f} GB" + '\n')

    filtered_df.columns = ['id', 'phone', 'email']
    filtered_df['id'] = filtered_df['id'].apply(lambda x: '' if x is None or len(x) < 3 else x)
    filtered_df['phone'] = filtered_df['phone'].apply(lambda x: '' if x is None or len(x) < 5 else x)
    filtered_df['email'] = filtered_df['email'].apply(lambda x: '' if x is None or len(x) < 5 else x)

    # Фильтруем строки, где хотя бы одно из 2 полей ('phone', 'email') не является пустой строкой
    filtered_df_phone_email = filtered_df[(filtered_df['phone'] != '') | (filtered_df['email'] != '')]

    # Преобразуем DataFrame в CSV-строку
    csv_string_phone_email = filtered_df_phone_email.to_csv(index=False, encoding='utf-8', sep=',')

    # Получение размеров CSV-строк в байтах
    size_bytes_phone_email = len(csv_string_phone_email.encode('utf-8'))

    # Перевод размеров из байтов в гигабайты
    size_gb_phone_email = size_bytes_phone_email / (1024 ** 3)

    # Вывод размеров CSV-строк
    print(f"Размер CSV-строки для phone_email: {size_gb_phone_email:.2f} GB")



    # ОТПРАВКА
    segments_id = ['__', '__', '__', '__']

    # Разделение датафрейма на 4 равные части
    num_chunks = 4
    chunk_size = len(filtered_df_phone_email) // num_chunks

    # Разбиение датафрейма на части и вычисление размера CSV-строки для каждой части
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = (i + 1) * chunk_size
        if i == num_chunks - 1:  # Последняя часть может быть немного больше, если длина не делится на num_chunks
            end_idx = None
        chunk_df = filtered_df_phone_email.iloc[start_idx:end_idx]
        unique_counts = chunk_df[['id', 'phone', 'email']].nunique()
        print(f"Количество уникальных значений для части {i+1}:\n {unique_counts}")

        csv_string_chunk = chunk_df.to_csv(index=False, encoding='utf-8', sep=',')
        size_bytes_chunk = len(csv_string_chunk.encode('utf-8'))
        size_gb_chunk = size_bytes_chunk / (1024 ** 3)
        print(f"Размер CSV-строки для части {i+1}: {size_gb_chunk:.2f} GB")
        print(f"segments_id[i]:{segments_id[i]}")

        #ИЗМЕНЕНИЕ СЕГМЕНТОВ ИЗ ДАННЫХ CRM
        # Заголовки запроса
        headers_change = {
        "Authorization": "OAuth {}".format(ym_token[0]),
        }

        # Параметры запроса
        params_change = {
        "hashed" : 1,
        "content_type" : 'crm' 
        }

        # URL
        url_change = f"https://api-audience.yandex.ru/v1/management/segment/{segments_id[i]}/modify_data?modification_type=replace"
        print(url_change)
        response_change = requests.post(url_change, files={'file': ('data.csv', csv_string_chunk.encode('utf-8'), 'text/csv; charset=utf-8')}, params=params_change, headers=headers_change) 
        print(response_change.text + '\n')


if __name__ == "__main__":
    # Глобальный счетчик для отслеживания успешно завершенных процессов
    counter = mp.Value('i', 0)

    num_processes=20

    # Очередь для передачи данных между процессами
    queue = Queue(maxsize=num_processes)
    print(f"Размер очереди queue сразу после создания: {queue.qsize()}", flush=True)

    # Начальная и конечная дата для выборки данных
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180) #180

    # Создание запросов для каждого процесса
    queries = create_queries(start_date, end_date, num_processes)

    prcss = [mp.Process(name=f'test_{r}', target=load_data, args=(queries[r]+(queue,))) for r in range(num_processes)]
    pnames = [p.name for p in prcss]  # имена процессов

    # Запустите все процессы
    for p in prcss:
        p.start()

    # Заставляем программу дождаться 10 объектов в очереди
    while queue.qsize() < num_processes:
        time.sleep(60)  # Подождем и проверим снова
        print('sleep 60 sec')
        pass


    # Объединение данных из всех процессов
    dfs = []
    while not queue.empty():
        dfs.append(queue.get())
    df_all_orderid_phone_email = pd.concat(dfs, ignore_index=True)

    # Вывод результата
    print(df_all_orderid_phone_email.head())
    print(df_all_orderid_phone_email.shape)
    
    process_and_send_data(df_all_orderid_phone_email)
        
    # Завершение работы всех процессов
    for process in prcss:
        process.terminate()
        process.join()
        
    queue.close()
            
            
    try:
        del dfs
        del df_all_orderid_phone_email
    except Exception as ex:
        print(ex)
