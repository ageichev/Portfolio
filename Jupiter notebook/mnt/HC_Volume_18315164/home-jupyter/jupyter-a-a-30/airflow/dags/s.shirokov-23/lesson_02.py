import requests
import pendulum
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_10_zones = df.zone.value_counts().head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(header=False))

        
def get_longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df.domain.str.len()
    longest_domain = df.sort_values(['length', 'domain'], ascending=[False, True]).head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False))

        
def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if 'airflow.com' not in df.domain.to_list():
        airflow_rank = 'Domain "airflow.com" is not in rank :('
    else:
        rank = df.query('domain == "airflow.com"')['rank'].values[0]
        airflow_rank = f'The domain "airflow.com" is {rank} in rank'
    return airflow_rank


def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    date = ds
    airflow_rank = get_airflow_rank()

    print(f'Top 10 zones for date {date}')
    print(top_10_zones)

    print(f'Longest domain for date {date}')
    print(longest_domain)
    
    print(airflow_rank)


default_args = {
    'owner': 's.shirokov-23',
    'depends_on_past': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=10),
    'start_date': pendulum.datetime(2022, 9, 1, tz='Europe/Moscow'),
    'end_date': pendulum.datetime(2022, 12, 31, tz='Europe/Moscow')
}


dag = DAG('s.shirokov-23_lesson_02', default_args=default_args, schedule_interval='0 12 * * *', catchup=False, tags=['airflow', 's.shirokov-23', 'lesson_02'])

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
