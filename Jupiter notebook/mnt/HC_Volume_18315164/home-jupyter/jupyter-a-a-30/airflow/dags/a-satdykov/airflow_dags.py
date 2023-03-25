import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# таски
# первый таск - забирает таблицу

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream = True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    
    with open(TOP_1M_DOMAINS_FILE,'w') as f:
        f.write(top_data)

# второй таск - соберём статистику

def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
    top_data_top_10 = top_data_top_10.head(10)
    
    with open ('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index = False, header = False))
        
        
# добавим ещё задачу посмотреть в зоне .COM

def get_stat_COM():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
    top_data_top_10 = top_data_top_10.head(10)
    
    with open ('top_data_top_10_com.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index = False, header = False))
        

# ну и третье задание - полученный файл и напием лог с заголовком

def print_data(ds):
    with open ('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open ('top_data_top_10_com.csv', 'r') as f:
        all_data_com = f.read()
        
    date = ds
    
    print(f'Top domains in .RU for date {date}')
    print (all_data)
    
    print(f'Top domains in .COM for date {date}')
    print (all_data_com)

# создаём теперь даг

default_args = {
    'owner': 'a-satdykov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 18),
    'schedule_interval': '0 12 * * *'
}


dag = DAG('top_10_RU', default_args = default_args)

# создаём таски

task_1 = PythonOperator(task_id = 'get_data', python_callable = get_data, dag = dag)

task_2 = PythonOperator(task_id = 'get_stat', python_callable = get_stat, dag = dag)

task_2_com = PythonOperator(task_id = 'get_stat_COM', python_callable = get_stat_COM, dag = dag)

task_3 = PythonOperator(task_id = 'print_data', python_callable = print_data, dag = dag)

# задаём зависимости между тасками для формирования графа

task_1 >> [task_2, task_2_com]  >> task_3

# или
# task_1.set_downstream(task_2)
# task_1.set_downstream(task_2_com)

# task_2.set_downstream(task_3)
# task_2_com.set_downstream(task_3)








