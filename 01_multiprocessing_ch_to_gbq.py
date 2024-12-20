# source /data/airflow/dags/venv/venv310airflow/bin/activate Активация окружения

# python3.10 /data/airflow/dags/git/scripts/ch_to_gbq_events_with_retro.py Запуск скрипта из GitLab

import time as t
import logging
import sys
import multiprocessing 
from clickhouse_driver import Client
import pandas as pd
import fastparquet
import requests
from datetime import datetime, date, timedelta, time
import os
from google.cloud import bigquery 
from google.oauth2 import service_account 
import signal
import gc
from queue import Empty
import json
import pandas_gbq as gbq
import tqdm



# Установка уровня логирования
logging.basicConfig(level=logging.INFO, stream=sys.stdout)


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



def alert(message):
    try:
        TOKEN = "___"
        chat_id = "____"
        message = message
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        requests.get(url).json()
    except Exception as ex:
        logging.error(f"Ошибка отправки сообщения через Telegram - {str(ex).upper()}")


@proxy_decorator
def df_datetime_bq_func():

    try:
        req_gbq='''
        SELECT MAX(ch_create_date) FROM `project.adhoc.ch_events` 
        '''

        df_datetime_bq = gbq.read_gbq(req_gbq, project_id=project_id, credentials=credentials, dialect='standard')
        
    except Exception as ex:
        logging.error(f"Ошибка чтения из GBQ - {str(ex).upper()}")
        alert(f"Ошибка - {str(ex)}")

    return df_datetime_bq




# Функция для получения данных из ClickHouse
def get_data_from_clickhouse(host_query):
    try:
        host, query = host_query
        client = Client(host, user=user_cred, password=password_cred)
        result_df = client.query_dataframe(query)
        result_df['shard'] = host
        return result_df
    except Exception as ex:
        logging.error(f"Ошибка get_data_from_clickhouse(host_query) - {str(ex).upper()}")
        alert(f"Ошибка get_data_from_clickhouse(host_query) - {str(ex)}")


# Функция для преобразования типов
def process_data(df):
    try:
        if df.shape[0] > 0:
            logging.info(f"df.shape {df.shape}")
            #преобразуем форматы столбцов по отдельности каждый
            df['adv_ad_code'] = df['adv_ad_code'].astype(str).apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8'))
            df['adv_ad_name'] = df['adv_ad_name'].astype(str).apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8'))
            df['adv_campaign_id'] = df['adv_campaign_id'].astype(str).apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8'))
            #...
            return df
    except Exception as ex:
        logging.error(f"Ошибка process_data(df) - {str(ex).upper()}")
        alert(f"Ошибка process_data(df) - {str(ex)}")


@proxy_decorator
def sent_df_to_bq(df):
    try:
        if df.shape[0] > 0:
            print(f'Processing df.shape[0] - {df.shape[0]}', flush=True)
            df = df.rename(columns={'list_name': 'listName'})
            df = df.rename(columns={'product_merchant_id': 'properties_merchant_id'})
            df = df.rename(columns={'product_bonus_amount': 'properties_bonuses'})
            
            df_schema = df.dtypes.reset_index()
            df_schema.columns = ['name', 'type']

            #преобразуем форматы столбцов по отдельности каждый
            df_schema.loc[df_schema['name'] == 'adv_ad_code', 'type'] = 'STRING'
            df_schema.loc[df_schema['name'] == 'adv_ad_name', 'type'] = 'STRING'
            df_schema.loc[df_schema['name'] == 'adv_campaign_id', 'type'] = 'STRING'
            #...

            dataset_id = 'adhoc'
            table_id = 'ch_events'
            table_ref = clientBQ.dataset(dataset_id).table(table_id)
            job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField(name=t.name, field_type=t.type) for t in df_schema.itertuples(index=False)], autodetect=False)
            
            # Загружаем из датафрейма
            job = clientBQ.load_table_from_dataframe(df, table_ref, job_config=job_config)
            logging.info(f"job.result() - {job.result()}.")
            # Проверяем успешность выполнения загрузки
            if job.state == 'DONE':
                logging.info(f"job.state - {job.state}.")
            else:
                # Обработка ошибки или логирование
                logging.info(f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}. 'Failed to load data to BigQuery' \n")
            del df
            gc.collect()  # Запустить сборщик мусора
        
        else:
            print(f'df.shape[0] с нулевым размером - {df.shape[0]}', flush=True)


    except Exception as e:
        logging.error(f"Error sent_df_to_bq: {str(e).upper()}")
        alert(f"Error sent_df_to_bq: {str(e)}")
        logging.error(f"sent_df_to_bq sleep 3600 sec (1 час) ...")
        t.sleep(3600)
        
        
               

#Функция воркер для загрузки данных из ClickHouse
def load_data_worker(input_queue, process_queue):
    logging.info("Starting load_data_worker")
    while True:
        try:
            host_query = input_queue.get()
            data = get_data_from_clickhouse(host_query)
            if not data.empty:
                process_queue.put(data)
                data = None  # Явное удаление объекта из памяти
                gc.collect()  # Запустить сборщик мусора
            
            else:
                logging.error(f"load_data_worker - data.empty")
            
            print(f"load_data_worker sleep 26 sec...", flush=True)
            t.sleep(26)
        except Empty:  # Используйте Empty для проверки пустой очереди
            continue
        except Exception as ex:
            logging.error(f"Ошибка load_data_worker - {str(ex).upper()}")
            continue

            
# Функция воркер для преобразования типов
def process_data_worker(process_queue, output_queue):
    logging.info("Starting process_data_worker")
    while True:
        try:
            data = process_queue.get()
            if not data.empty:
                processed_data = process_data(data)
                output_queue.put(processed_data)
                data = None  # Явное удаление объекта из памяти
                gc.collect()  # Запустить сборщик мусора
            else:
                logging.error(f"process_data_worker - data.empty")
        except Empty:  # Используйте Empty для проверки пустой очереди
            continue
        except Exception as ex:
            logging.error(f"Ошибка load_data_worker - {str(ex).upper()}")
            continue

            
# Функция воркер для сохранения в parquet файлы
def load_to_bq_worker(output_queue):
    logging.info("Starting load_to_bq_worker")
    while True:
        try:
            data = output_queue.get()
            if not data.empty:
                sent_df_to_bq(data)
                data = None  # Явное удаление объекта из памяти
                gc.collect()  # Запустить сборщик мусора
            
                print(f"load_to_bq_worker sleep 26 sec...", flush=True)
                t.sleep(26)
                
            else:
                logging.error(f"load_to_bq_worker - data.empty")
    
        except Empty:  # Используйте Empty для проверки пустой очереди
            continue
        except Exception as ex:
            logging.error(f"Ошибка load_to_bq_worker - {str(ex).upper()}")
            continue      
    

def terminate_processes(processes):
    for p in processes:
        p.terminate()

def create_queues():
    input_queue = multiprocessing.Queue(maxsize=16)
    process_queue = multiprocessing.Queue(maxsize=16)
    output_queue = multiprocessing.Queue(maxsize=16)
    return input_queue, process_queue, output_queue

def setup_processes(input_queue, process_queue, output_queue):
    num_processes = 16  
    load_processes = [multiprocessing.Process(target=load_data_worker, args=(input_queue, process_queue)) for _ in range(num_processes)]
    process_processes = [multiprocessing.Process(target=process_data_worker, args=(process_queue, output_queue)) for _ in range(num_processes)]
    save_processes = [multiprocessing.Process(target=load_to_bq_worker, args=(output_queue,)) for _ in range(num_processes)]
    return load_processes, process_processes, save_processes



def multiproc_func(event_date, datetime_start_part, seconds_part, input_queue, process_queue, output_queue, load_processes, process_processes, save_processes):
    logging.info("Starting multiproc_func")
    logging.info(f"event_date - {event_date}, datetime_start_part - {datetime_start_part}, seconds_part - {seconds_part}")
    
    once_on_the_fourth_part = timedelta(seconds=seconds_part / 4)

    # Вывод размеров очередей.
    print(f"Размер очереди input_queue сразу после создания: {input_queue.qsize()}", flush=True)
    print(f"Размер очереди process_queue сразу после создания: {process_queue.qsize()}", flush=True)
    print(f"Размер очереди output_queue сразу после создания: {output_queue.qsize()}", flush=True)

    queries = []
    for i in range(4):
        start_time = datetime_start_part + once_on_the_fourth_part * i
        end_time = start_time + once_on_the_fourth_part
        query = f'''
        SELECT hash_key, adv_ad_code, adv_ad_name, adv_campaign_id, (...), ch_create_date
        FROM events
        WHERE event_date = '{event_date}'
          AND ch_create_date > '{start_time}'
          AND ch_create_date <= '{end_time}'
        '''
        queries.append(query)

    return queries

                

@proxy_decorator
def df_maxdatetime_bq_retro_func():
    try:
        req_gbq='''
        SELECT MAX(ch_create_date) FROM `project.adhoc.ch_events` 
        '''

        df_maxdatetime_bq = gbq.read_gbq(req_gbq, project_id=project_id, credentials=credentials, dialect='standard')
    except Exception as ex:
        logging.error(f"Ошибка чтения из GBQ - {str(ex).upper()}")
        alert(f"retro_func - Ошибка чтения из GBQ - {str(ex)}")

    return df_maxdatetime_bq


@proxy_decorator
def df_gbq_retro_func(event_date, df_maxdatetime_bq):
    try:
        print(f'event_date - {event_date}')
        
        req_gbq=f'''SELECT 
        TIMESTAMP_TRUNC(ch_create_date, MINUTE) as minute_start,
        COUNT(DISTINCT hash_key) as hash_key
        FROM project.adhoc.ch_events
        WHERE event_date = '{event_date}'
        GROUP BY minute_start
        ORDER BY minute_start
        '''

        df_gbq = gbq.read_gbq(req_gbq, project_id=project_id, credentials=credentials, dialect='standard')

        df_gbq = df_gbq[df_gbq['minute_start'].dt.date == pd.to_datetime(event_date).date()]
        df_gbq = df_gbq[df_gbq['minute_start'] < df_maxdatetime_bq.iloc[0,0] - timedelta(minutes=30)]

        logging.info(f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}. df_gbq.shape[0] - {df_gbq.shape[0]}")
        logging.info(f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}. GBQ OK")

    except Exception as ex:
        logging.error(f"retro_func - Ошибка чтения из GBQ - {str(ex).upper()}")
        alert(f"retro_func - Ошибка чтения из GBQ - {str(ex)}")
    
    return df_gbq
        
      

def retro_func(input_queue, process_queue, output_queue, load_processes, process_processes, save_processes): #здесь нужно передать параметры
    try:

        df_maxdatetime_bq = df_maxdatetime_bq_retro_func()

        df_minutes_to_download = pd.DataFrame()

        # Определяем глубину проверки в днях
        depth = 7  # Количество дней назад начиная с текущего
        event_date_today = pd.to_datetime(datetime.now()).date()
        list_event_date = [(event_date_today - timedelta(days=i)) for i in range(depth)]
       

        for event_date in list_event_date:

            df_gbq = df_gbq_retro_func(event_date, df_maxdatetime_bq)
            
            try:  
                req_ch=f'''SELECT 
                toStartOfMinute(ch_create_date) as minute_start,
                uniqExact(hash_key) as hash_key
                FROM ssa_eds.events_r
                WHERE event_date = '{event_date}'
                GROUP BY minute_start
                ORDER BY minute_start
                '''

                df_ch_list = [get_data_from_clickhouse([host, req_ch]) for host in clickhouse_hosts_new]
                # Соединяем все четыре датафрейма в один
                merged_df = pd.concat(df_ch_list)
                # Удаляем столбец 'shard'
                merged_df.drop(columns='shard', inplace=True)
                # Преобразуем столбец 'minute_start' в формат datetime
                merged_df['minute_start'] = pd.to_datetime(merged_df['minute_start'], format='%Y%m%d%H%M%S')
                # Фильтруем данные по дате события (event_date)
                merged_df = merged_df[merged_df['minute_start'].dt.date == pd.to_datetime(event_date).date()]
                # Фильтруем данные по условию, что 'minute_start' меньше максимальной даты минус 30 минут
                merged_df = merged_df[merged_df['minute_start'] < df_maxdatetime_bq.iloc[0,0] - pd.Timedelta(minutes=30)]
                # Группируем данные по 'minute_start' и суммируем значения 'device_count', 'user_count' и 'session_count'
                df_ch = merged_df.groupby('minute_start').sum().reset_index()

                logging.info(f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}. df_ch.shape[0] - {df_ch.shape[0]}")
                logging.info(f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}. CH OK")
            except Exception as ex:
                logging.error(f"retro_func - Ошибка чтения из CH - {str(ex).upper()}")
                alert(f"retro_func - Ошибка чтения из CH - {str(ex)}")


            
            # Находим различающиеся строки
            diff_df1 = df_gbq.merge(df_ch, indicator=True, how='outer').loc[lambda x : x['_merge'] == 'left_only']
            diff_df2 = df_ch.merge(df_gbq, indicator=True, how='outer').loc[lambda x : x['_merge'] == 'left_only'] 
            result_df_diff = pd.concat([diff_df1, diff_df2], ignore_index=True)
            result_df_diff = result_df_diff.drop_duplicates(subset=['minute_start'])


            df_minutes_to_download = pd.concat([df_minutes_to_download, result_df_diff])
            print(df_minutes_to_download)
            print('------------')

        df_minutes_to_download = df_minutes_to_download.sort_values(by=['minute_start'], ascending = False)
        print(df_minutes_to_download)
        t.sleep(20)


        #Помещаем задачи в очередь   
        for index, row in df_minutes_to_download.iterrows():
            seconds_part_retro = 60 #регулируем загружаемые части events (20, 60), нужно чтобы делились нацело на 3.
            event_date_retro = pd.to_datetime(row['minute_start']).date()
            datetime_start_part_retro = pd.to_datetime(row['minute_start']) 
            once_on_the_fourth_part_retro = timedelta(seconds=seconds_part_retro/4)

            print(event_date_retro)
            print(datetime_start_part_retro)
            print(datetime_start_part_retro+once_on_the_fourth_part_retro)

            print(event_date_retro)
            print(datetime_start_part_retro + once_on_the_fourth_part_retro)
            print(datetime_start_part_retro + once_on_the_fourth_part_retro*2)

            print(event_date_retro)
            print(datetime_start_part_retro + once_on_the_fourth_part_retro*2)
            print(datetime_start_part_retro + once_on_the_fourth_part_retro*3)

            print(event_date_retro)
            print(datetime_start_part_retro + once_on_the_fourth_part_retro*3)
            print(datetime_start_part_retro + once_on_the_fourth_part_retro*4)

            print('------------')

            #здесь передаем параметры ретро блока
            multiproc_func(event_date_retro, datetime_start_part_retro, seconds_part_retro, input_queue, process_queue, output_queue, load_processes, process_processes, save_processes)
            

    except Exception as ex:
        logging.error(f"Ошибка retro_func - {str(ex).upper()}")
        alert(f"Ошибка retro_func - {str(ex)}")


        
# Главная функция           
def main():
    try:
        event_date = df_datetime_bq.iloc[0,0].strftime('%Y-%m-%d') 
        start_datetime_event = df_datetime_bq.iloc[0,0] 
        datetime_start_part = start_datetime_event #далее каждую итерацию меняем в конце цикла
        seconds_part = 60 #регулируем загружаемые части events (10, 20,30,40, 60)

        last_run_time_1_2 = None  
        last_run_time_3_4 = None 

        input_queue, process_queue, output_queue = create_queues()
        load_processes, process_processes, save_processes = setup_processes(input_queue, process_queue, output_queue)

        for p in load_processes + process_processes + save_processes:
            p.start()

        #Помещаем задачи в очередь
        while True: 
            try:
                current_time = datetime.now().time()
                time1 = time(22, 0)
                time2 = time(23, 0)
                time3 = time(1, 0)
                time4 = time(2, 0)

                if time1 <= current_time <= time2:
                    if last_run_time_1_2 is None or (datetime.now() - last_run_time_1_2).days >= 1:
                        print(f"Текущее время находится в интервале от {time1} до {time2} и ретро блок будет выполнен.", flush=True)

                        #включаем блок ретро догрузки
                        retro_func(input_queue, process_queue, output_queue, load_processes, process_processes, save_processes)
                        last_run_time_1_2 = datetime.now()

                    else:
                        print(f"Текущее время находится в интервале от {time1} до {time2}, но ретро блок уже был выполнен сегодня. Продолжаем выполнение основного скрипта.", flush=True)

                elif time3 <= current_time <= time4:
                    if last_run_time_3_4 is None or (datetime.now() - last_run_time_3_4).days >= 1:
                        print(f"Текущее время находится в интервале от {time3} до {time4} и ретро блок будет выполнен.", flush=True)
                        #включаем блок ретро догрузки
                        retro_func(input_queue, process_queue, output_queue, load_processes, process_processes, save_processes)
                        last_run_time_3_4 = datetime.now()
                    else:
                        print(f"Текущее время находится в интервале от {time3} до {time4}, но ретро блок уже был выполнен сегодня. Продолжаем выполнение основного скрипта.", flush=True)


                #Проработать еще переход в полночь с одного дня на другой
                while pd.to_datetime(datetime.now()) - datetime_start_part < timedelta(seconds=600):
                    logging.info(f"{datetime_start_part} Мы подошли к рилтайму. Sleep 60 сек...")
                    t.sleep(60)

                multiproc_func(event_date, datetime_start_part, seconds_part, input_queue, process_queue, output_queue, load_processes, process_processes, save_processes)
                datetime_start_part = datetime_start_part + timedelta(seconds=seconds_part) #каждую ОК итерацию меняем вперед
                event_date = datetime_start_part.strftime('%Y-%m-%d') 
            except Exception as ex:
                logging.error(f"Ошибка в main() функции - {str(ex).upper()}")
                t.sleep(10)

        
    except Exception as ex:
        logging.error(f"Ошибка в main() функции - {str(ex).upper()}")
        alert(f"Ошибка в main() функции - {str(ex)}")
            
    finally:
        # Завершение работы воркеров
        terminate_processes(load_processes + process_processes + save_processes)
        for p in load_processes + process_processes + save_processes:
            p.join()

       
        input_queue.close()
        process_queue.close()
        output_queue.close()
        
        # Вывод размеров очередей.
        print(f"Размер очереди input_queue в конце итерации цикла: {input_queue.qsize()}", flush=True)
        print(f"Размер очереди process_queue в конце итерации цикла: {process_queue.qsize()}", flush=True)
        print(f"Размер очереди output_queue в конце итерации цикла: {output_queue.qsize()}", flush=True)

            
if __name__ == "__main__":

    initial_path = '/data/'

    path_server_configs = F'{initial_path}airflow/dags/configs/'
    with open(f'{path_server_configs}proxies.json', 'r') as f:
        proxies = json.load(f)

    path_server = '/data/airflow/dags/ch_to_gbq_events/'

    credentials = service_account.Credentials.from_service_account_file(f'{path_server}gbq.json') #креды BQ
    project_id = 'project'
    clientBQ = bigquery.Client(credentials=credentials,project=project_id)

    cred = pd.read_json(f'{path_server}cred.json', typ='series')
    user_cred = cred.iloc[0]
    password_cred = cred.iloc[1] 
    
    # Новый путь к временной директории
    os.environ['TMPDIR'] = '/data/airflow/dags/tmp_mgr' 

    df_datetime_bq = df_datetime_bq_func()

    clickhouse_hosts_new = ['msk.host01.local',
                            'msk.host02.local',
                            'msk.host03.local',
                            'msk.host04.local']

    main()
