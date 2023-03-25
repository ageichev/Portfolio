import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Указание путей к данным
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# Объявление функций, которые будут выполняться в тасках
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.str.split('.').str[-1]
    top_data_top_zone_10 = top_data_df.groupby('zone', as_index= False).agg({'domain':'count'})
    top_data_top_zone_10 = top_data_top_zone_10.sort_values('domain', ascending= False).head(10)
    with open('top_data_top_zone_10.csv', 'w') as f:
        f.write(top_data_top_zone_10.to_csv(index=False, header=False))


def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.str.len()
    longest_name_df = top_data_df.sort_values(['length', 'domain'], ascending= [False, True]).head(1)
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name_df.to_csv(index=False, header=False))


def get_domain_rank():
    domain_name = 'airflow.com'
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_data = top_data_df.query('domain == @domain_name')
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_data.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_top_zone_10.csv', 'r') as f:
        all_data_zone = f.read()
    with open('longest_name.csv', 'r') as f:
        the_longest_domain_name = f.read().split(',')[1]
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_df = f.read()
        if len(airflow_rank_df) == 0:
            airflow_rank = 'not available'
        else:
            airflow_rank = airflow_rank_df.split(',')[0]

    date = ds

    print(f'Top zones for date {date}')
    print(all_data_zone)

    print(f'The longest domain name for date {date} is')
    print(the_longest_domain_name)
    print()

    print(f'airflow.com rank for date {date} is')
    print(airflow_rank)

# Инициализация DAG
default_args = {
    'owner': 'r-kalinin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 11),
    'schedule_interval': '0 */6 * * *'
}
dag = DAG('lesson_2_rk', default_args=default_args)

# Инициализация тасков
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag)

t2_name = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_domain_rank',
                        python_callable=get_domain_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

# Задание порядка выполнения
t1 >> [t2_zone, t2_name, t2_rank] >> t3