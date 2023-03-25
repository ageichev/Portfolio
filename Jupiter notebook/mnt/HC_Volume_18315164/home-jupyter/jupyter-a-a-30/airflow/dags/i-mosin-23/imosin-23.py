

import requests #это модуль Python, который вы можете использовать для отправки всех видов HTTP-запросов
import pandas as pd
import airflow
import numpy as np

from datetime import timedelta # будем запускаться раз в 5 минут
from datetime import datetime 

from airflow import DAG #создладим даг
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip' #cсылка на файл
TOP_1M_DOMAINS_FILE = 'top-1m.csv' # как он будет называться

# первый таск - считывает файл
def get_data():
    # (Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке)
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f: #сохраняем его на диск 
        f.write(top_data) #навзание


def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) #задаем имена, какой номер сайта в топе самых популярных, далее домен
    top_data_df['part'] = top_data_df.domain.str.split('.', expand=True)[1]
    top_data_top_10 = top_data_df.groupby('part').count().sort_values(by='rank',ascending=False).head(10) #возьмем только домены в зоне .ru
    with open('top_data_top_10.csv', 'w') as f: #сохраним это в файл 
        f.write(top_data_top_10.to_csv(index=False, header=False)) #обрато сделаем из него csv, индекс нам ненужен, заголовк тоже 
        
def get_max_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) #задаем имена, какой номер сайта в топе самых популярных, далее домен
    top_data_df['length'] = top_data_df.domain.str.len()
    length_max = top_data_df.sort_values(by='length', ascending=False).max() #возьмем только домены в зоне .ru
    with open('length_max.csv', 'w') as f: #сохраним это в файл 
        f.write(length_max.to_csv(index=False, header=False)) #обрато сделаем из него csv, индекс


def index_air():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank_s = df.loc[df.domain == 'airflow.com', 'rank']
    if airflow_rank_s.empty:
        result = 'domain airflow.com is not in list'
    else:
        result = airflow_rank_s.item()
    with open('airflow_rank.txt', 'w') as f:
        f.write(result)

#3) Финальный таск должен писать в лог результат ответы на вопросы выше
# cчитваем файл и выдаем информцию 
def print_data(ds):
    with open('get_top_10.csv', 'r') as f: #cчитваем данные - r 
        all_top_10 = f.read()
    with open('length_max.csv', 'r') as f: #считываем данные с cjm
        my_max = f.read()
    with open('index_air.csv', 'r') as f: #считываем данные с cjm
        my_index = f.read()
    date = ds

    print(f'Top domains in for date {date}') #заголовок 
    print(all_top_10) # таблица

    print(f'Max domain for date {date}')
    print(my_max)
    
    print(f'Index  {date}')
    print(my_index)


#параметры нашего дага 
default_args = {
    'owner': 'i.mosin23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 8, 30),
}
schedule_interval = '0 10 * * *' #зададим крон 

dag = DAG('ilya-mosin-dag-23', default_args=default_args, schedule_interval=schedule_interval)



t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len',
                        python_callable=get_max_len,
                        dag=dag)

t4 = PythonOperator(task_id='index_air',
                        python_callable=index_air,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5 #порядко залдания дага 
#еще один вариант
#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)





