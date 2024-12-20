import multiprocessing as mp
from multiprocessing import Process, Queue, JoinableQueue, Semaphore
import numpy as np
import clickhouse_connect
import pandas as pd
import pandas_gbq as gbq
import tqdm
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import gc
import uuid
import json
import re
import requests
import fastparquet 

import sys
import fcntl
import time
import pwd
import functools
import signal
import os
import shutil

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
        print(f"{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.PID, USER текущего процесса:{pid}, {user}")
    except OSError as e:
        print(f"{e}")
        if e.errno == 11:
            pid = os.getpid()
            user = pwd.getpwuid(os.geteuid())[0]
            print("Дубликат скрипта уже запущен")
            sys.exit(0)
            os._exit(1)  # Добавляем os._exit(1) для немедленного завершения скрипта
        else:
            raise

def is_valid_uuid(uuid_str):
    pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
    return pattern.match(uuid_str) is not None

def is_numeric_string(value):
    return value.isdigit() if isinstance(value, str) else False

def is_valid_date(date_str):
    # Пропускаем строки, которые равны 'NaT' или пустые
    if pd.isna(date_str) or date_str == 'NaT':
        return True
    try:
        # Проверяем, можно ли преобразовать строку в объект datetime
        datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        return True
    except (ValueError, TypeError):
        return False



# фиксируем успешно отправленные запросы 
def fix_sent_requests(df_successfully_sent):
    if df_successfully_sent.shape[0] > 0:
        
        df_successfully_sent = df_successfully_sent.astype(str)
        
        df_sent_data_old = pd.read_parquet(f'{path_server_file}sent_data_v2.parquet', engine='fastparquet')

         # валидация click_id на паттерн uuid и исключение некорректных строк
        df_sent_data_old = df_sent_data_old[df_sent_data_old['click_id'].apply(is_valid_uuid)]
        print(f'df_sent_data_old.shape_after_uuid_validation - {df_sent_data_old.shape}')
    
        # Валидация pid и login на числовые значения
        df_sent_data_old = df_sent_data_old[df_sent_data_old['pid'].apply(is_numeric_string)]
        df_sent_data_old = df_sent_data_old[df_sent_data_old['login'].apply(is_numeric_string)]
        print(f'df_sent_data_old.shape_after_numeric_validation - {df_sent_data_old.shape}')

        
        df_sent_data_old = df_sent_data_old[df_sent_data_old.duplicated(subset=['login', 'pid', 'click_id', 'amount', 'cash_id'], keep='first') == False].reset_index(drop=True)
        print(f'df_sent_data_old.shape_after_drop_duplicates_keep_first_by_columns - {df_sent_data_old.shape}')

        df_successfully_sent_new = pd.concat([df_sent_data_old, df_successfully_sent], sort=False, ignore_index=True, axis=0)


        df_successfully_sent_new = df_successfully_sent_new.astype(str)
    

        # Определяем путь к файлу и временный файл
        file_path = f'{path_server_file}sent_data_v2.parquet'
        temp_file = f'{path_server_file}sent_data_v2_temp.parquet'
        
        with open(file_path, 'wb') as f:
            # Используем блокировку для предотвращения одновременного доступа
            fcntl.flock(f, fcntl.LOCK_EX)
            
            # Сохраняем данные во временный файл
            df_successfully_sent_new.to_parquet(temp_file, engine='fastparquet')
            
            # Заменяем основной файл временным
            os.replace(temp_file, file_path)
            

        print(f"{datetime.now().strftime('%Y.%m.%d_%H:%M:%S')}.Размер датафрейма сохраненных отправок {df_successfully_sent_new.shape}")
        
        del df_successfully_sent
        del df_successfully_sent_new
        del df_sent_data_old
        
    else:
        print(f"{datetime.now().strftime('%Y.%m.%d_%H:%M:%S')}.Успешных отправок нет. Размер датафрейма успешно отправленных данных - {df_successfully_sent.shape[0]}")


    

def send_data_part(df_for_send):
    # отправляем запросы
    if df_for_send.shape[0] > 0:
        n = 0
        columns = ['action', 'login', 'datetime', 'date','pid', 'click_id', 'amount','cash_id',
        'hash_event', 'request_url', 'request_payload','request_datetime','response_status_code','response_text']
        buffer_df = pd.DataFrame(columns=columns)  # Пустой датафрейм для накопления данных


        for index, row in df_for_send.iterrows():
            try:
                # Преобразование строки в объект datetime
                dt = datetime.fromisoformat(row['datetime'])
                
                # Форматирование объекта datetime в нужный формат
                # formatted_datetime = dt.strftime("%Y%m%d_%H%M")
                formatted_datetime = dt.strftime('%Y-%m-%dT%H:%M:%S') + '-0700'

                print("\n" + "-"*50 + "\n")
                if row['action'] == 'registration':
                # if row['ftd_datetime'] == 'NaT':
                    url = f"https://host.com/company?click_id={row['click_id']}&secret=Khbmn23r&action=reg&datetime={formatted_datetime}"
                    payload = None  # нет полезной нагрузки для регистрации
                    response = requests.post(url)
                elif row['action'] == 'deposit':
                # elif row['ftd_datetime'] != 'NaT':
                    url = f"https://host.com/company?click_id={row['click_id']}&secret=Khbmn23r&action=dep&txId={row['cash_id']}&datetime={formatted_datetime}&login={row['login']}"
                    payload = {'amount': row['amount'], 'currency': 'RUB'}
                    response = requests.post(url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})

                
                print("URL запроса:", url)
                print("Payload:", json.dumps(payload) if payload else 'N/A')
                print("Статус код:", response.status_code)
                print("Текст ответа:", response.text)
                print(f'n == {n}')
                
                # Проверка на статус 200 или наличие "Already processed" в ответе
                try:
                    response_json = response.json()
                    error_message = response_json.get("error", "")
                except json.JSONDecodeError:
                    error_message = ""

                if response.status_code == 200 or error_message == "Already processed":
                    
                    # Добавляем успешно отправленные данные в буфер
                    new_row = row.copy()  # Копируем исходную строку
                    new_row['request_url'] = url
                    new_row['request_payload'] = json.dumps(payload) if payload else 'N/A'
                    new_row['request_datetime'] = datetime.now() + timedelta(hours = 3)
                    new_row['response_status_code'] = response.status_code
                    new_row['response_text'] = response.text

                    # Добавляем новую строку в buffer_df
                    buffer_df = pd.concat([buffer_df, pd.DataFrame([new_row], columns=columns)], ignore_index=True)
        
                else:
                    print(f"{datetime.now().strftime('%Y.%m.%d_%H:%M:%S')}, {url}. Запрос не ОК. {response.text}")

                time.sleep(0.004)  # задержка
                n += 1
                
            except Exception as e:
                print(f"Ошибка в процессе обработки строки: {e}")
                continue
                
        return buffer_df


def process_data(df_gbq):
    num_parts = 10
    df_parts = np.array_split(df_gbq, num_parts)

    with mp.Pool(processes=num_parts) as pool:
        results = pool.map(send_data_part, df_parts)

    # Объединяем результаты
    df_all_successfully_sent = pd.concat(results, ignore_index=True)

    # Записываем все успешно отправленные данные в файл
    fix_sent_requests(df_all_successfully_sent)

if __name__ == "__main__":  

    path_lockfiles = '/home/airflow/lockfiles/'
    path_server = '/home/airflow/cred/'
    path_server_file = '/home/airflow/scripts_files/sent_data/'
    
    lock_file = f'{path_lockfiles}gbq_send_data_v2.lock'
    prevent_duplicates(lock_file)

    # Создание резервной копии сохраняемых отправок
    backup_file = f'{path_server_file}sent_data_v2_backup.parquet'
    shutil.copy(f'{path_server_file}sent_data_v2.parquet', backup_file)
    
    credentials = service_account.Credentials.from_service_account_file(f'{path_server}gbq.json') #креды BQ
    project_id = 'project'
    clientBQ = bigquery.Client(credentials=credentials,project=project_id)

    columns = [
    'action',
    'login',
    'datetime',
    'date',
    'pid',
    'click_id',
    'amount',
    'cash_id',
    'hash_event'
    ]

    # Создаем строку с именами колонок, разделенными запятыми
    columns_str = ', '.join(columns)

    req_gbq=f'''
    SELECT {columns_str} FROM `project.attribution.postback` 
    WHERE hash_event IS NOT NULL
    '''
    df_gbq = gbq.read_gbq(req_gbq, project_id=project_id, credentials=credentials, dialect='standard')
    print (f'df_gbq.shape - {df_gbq.shape}')
    
    # валидация click_id на паттерн uuid и исключение некорректных строк
    df_gbq = df_gbq[df_gbq['click_id'].apply(is_valid_uuid)]
    print(f'df_gbq.shape_after_uuid_validation - {df_gbq.shape}')

    # Валидация pid и login на числовые значения
    df_gbq = df_gbq[df_gbq['pid'].apply(is_numeric_string)]
    df_gbq = df_gbq[df_gbq['login'].apply(is_numeric_string)]
    print(f'df_gbq.shape_after_numeric_validation - {df_gbq.shape}')

    
    df_gbq = df_gbq[df_gbq.duplicated(subset=['login', 'pid', 'click_id', 'amount', 'cash_id'], keep='first') == False].reset_index(drop=True)
    print (f'df_gbq.shape_after_drop_duplicates_keep_first - {df_gbq.shape}')
    
    df_gbq = df_gbq.astype(str)


    # "вычитаем" из датафрейма GBQ уже отправленные ранее данные
    file_parquet = f'{path_server_file}sent_data_v2.parquet'
    isExist = os.path.isfile(file_parquet)

    if isExist:
        print('Файл parquet с ранее отправленными данными обнаружен, вычитаем их.')

        df_send_data_old = pd.read_parquet(f'{path_server_file}sent_data_v2.parquet', engine='fastparquet').reset_index(drop=True)
        print(f'df_send_data_old.shape - {df_send_data_old.shape}')

        # валидация click_id на паттерн uuid и исключение некорректных строк
        df_send_data_old = df_send_data_old[df_send_data_old['click_id'].apply(is_valid_uuid)]
        print(f'df_send_data_old.shape_after_uuid_validation - {df_send_data_old.shape}')
    
        # Валидация pid и login на числовые значения
        df_send_data_old = df_send_data_old[df_send_data_old['pid'].apply(is_numeric_string)]
        df_send_data_old = df_send_data_old[df_send_data_old['login'].apply(is_numeric_string)]
        print(f'df_send_data_old.shape_after_numeric_validation - {df_send_data_old.shape}')

        #удалялем дубли из сохраненных отправок в parquet, оставляя только первые вхождения строк
        df_send_data_old = df_send_data_old[df_send_data_old.duplicated(subset=['login', 'pid', 'click_id', 'amount', 'cash_id'], keep='first') == False].reset_index(drop=True)
        df_send_data_old = df_send_data_old.astype(str) 
        print (f'df_send_data_old.shape_after_drop_duplicates_keep_first - {df_send_data_old.shape}')


        df_for_send = df_gbq[~df_gbq['hash_event'].isin(df_send_data_old['hash_event'])]
        
        # Печать размера итогового DataFrame
        print(f'df_for_send.shape_after_filtering - {df_for_send.shape}')


        if 0 < df_for_send.shape[0] < df_gbq.shape[0]+10:
            process_data(df_for_send[:2000])
        else:
            print(f"{datetime.now().strftime('%Y.%m.%d_%H:%M:%S')}. Нет новых транзакций для отправки. {df_for_send.shape[0]}")
    
    else:
        print('Файл parquet с ранее отправленными данными не обнаружен.')


    try:
        del df_for_send
        del df_gbq
        del df_send_data_old
        del file_parquet
        gc.collect()
    except Exception as ex:
        print(ex)
