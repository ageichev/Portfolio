import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_FILE = 'top-1m.csv'

# Задаем Таски. Каждая таска - функция.
def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_FILE, 'w') as f:
        f.write(top_data)


def get_top_zones():
    top_data_df = pd.read_csv(TOP_1M_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_zones = top_data_df\
        .groupby('domain_zone')\
        .agg({'domain':'count'})\
        .sort_values('domain', ascending=False)\
        .reset_index()\
        .head(10)
    with open('top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))


def max_len_domain():
    top_data_df = pd.read_csv(TOP_1M_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df['domain'].apply(lambda x: len(x))
    len_max_domain = top_data_df.sort_values(['len_domain', 'domain'], ascending=[False, True]).head(1)
    len_max_domain_name = len_max_domain['domain']
    with open('len_max_domain_name.csv', 'w') as f:
        f.write(len_max_domain_name.to_csv(index=False, header=False))


def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain.str.endswith("airflow.com")').reset_index()
    airflow_rank_num = airflow_rank['rank']
    with open('airflow_rank_num.csv', 'w') as f:
        f.write(airflow_rank_num.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_zones.csv', 'r') as f:
        zones = f.read()
    with open('len_max_domain_name.csv', 'r') as f:
        domain = f.read()
    with open('airflow_rank_num.csv', 'r') as f:
        air_rank = f.read()
    date = ds

    print(f'Top zones for date {date}')
    print(zones)

    print(f'The largest domain for date {date}')
    print(domain)

    print(f'For date {date}')
    print(f'The airflow.com`s rank is {air_rank}')

# Инициация DAGa
default_args = {
    'owner': 'a.novoselov-15',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 16),
}
schedule_interval = '0 22 * * *'

dag = DAG('less_2_novoselov', default_args=default_args, schedule_interval=schedule_interval)

# Инициализируем таски

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag)

t2_len = PythonOperator(task_id='max_len_domain',
                        python_callable=max_len_domain,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_len, t2_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
