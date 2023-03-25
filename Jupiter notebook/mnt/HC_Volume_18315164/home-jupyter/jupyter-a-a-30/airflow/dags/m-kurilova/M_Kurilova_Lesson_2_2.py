# импортируем библиотеки
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# подгружаем данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# создаем датасет, предварительно выгрузив из архива
top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
zipfile = ZipFile(BytesIO(top_doms.content))
top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

# создаем коды для тасков
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_zone_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone_domain'] = top_data_df['domain'].str.split('.').str[-1]
    top_10_zone_domain = top_data_df.groupby('zone_domain').agg({'domain': 'count'})\
                                    .sort_values('domain', ascending=False)\
                                    .rename(columns={'domain' : 'count_domain'})\
                                    .head(10)
    with open('top_10_zone_domain.csv', 'w') as f:
        f.write(top_10_zone_domain.to_csv(index=False, header=False))


def get_large_name_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length_domain'] = top_data_df['domain'].str.len()
    longest_length_domain = top_data_df.sort_values(['length_domain', 'domain'], ascending=[False, True])\
                                       .head(1)['domain'].max() 
    with open('longest_length_domain.csv', 'w') as f:
        f.write(str(longest_length_domain))
       
    
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    filter_airflow = top_data_df ['domain']. isin (['airflow.com'])
    airflow_rank = top_data_df[filter_airflow ]['rank'].max()
    if airflow_rank >= 1:
        airflow_rank = top_data_df[filter_airflow ]['rank'].max()
    else:
        airflow_rank = 'not on the list'
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank)    
    
    
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_zone_domain.csv', 'r') as f:
        top_10_zone_domain = f.read()
    with open('longest_length_domain.csv', 'r') as f:
        longest_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()        
        
    date = ds

    print(f'Top 10 zone domains for date {date}')
    print(top_10_zone_domain)

    print(f'The longest domain for date {date}')
    print(longest_length)
    
    print(f'The rank of domain airflow.com for date {date} is:')
    print(airflow_rank)

        
# создаем DAG
default_args = {
    'owner': 'm-kurilova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 22),
    'schedule_interval': '0 0 * * *'
}

m_kurilova = DAG('m-kurilova_lesson_2', default_args=default_args)

# инициализируем таски
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=m_kurilova)

t2 = PythonOperator(task_id='get_top_10_zone_domain',
                    python_callable=get_top_10_zone_domain,
                    dag=m_kurilova)

t3 = PythonOperator(task_id='get_large_name_domain',
                        python_callable=get_large_name_domain,
                        dag=m_kurilova)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=m_kurilova)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=m_kurilova)

# задаем логику выполнения
t1 >> [t2, t3, t4] >> t5 