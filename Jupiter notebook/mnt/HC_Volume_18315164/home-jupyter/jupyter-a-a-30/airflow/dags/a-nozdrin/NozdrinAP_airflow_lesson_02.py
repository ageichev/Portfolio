import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_domain_zones():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE)
    top_doms['domain_zone'] = top_doms['domain'].apply(lambda x: x.split(".")[-1])
    top_10_domain_zones = top_doms.groupby('domain_zone').agg(count=('domain_zone','count')).nlargest(10, columns='count', keep='all')
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv())

def get_domain_with_max_len():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE)
    domain_with_max_len = top_doms[top_doms['domain'].str.len() == top_doms['domain'].str.len().max()].sort_values('domain').head(1).domain
    with open('domain_with_max_len.csv', 'w') as f:
        f.write(domain_with_max_len.to_csv(index=False, header=False))

def get_rank_airflow_com():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE)
    rank_airflow_com = top_doms.query('domain == "airflow.com"')['rank']
    with open('rank_airflow_com.csv', 'w') as f:
        f.write(rank_airflow_com.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
    with open('domain_with_max_len.csv', 'r') as f:
        domain_with_max_len = f.read()
    with open('rank_airflow_com.csv', 'r') as f:
        rank_airflow_com = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_domain_zones)

    print(f'Domain with max len for date {date}')
    print(domain_with_max_len)

    print(f'Rank airflow.com for date {date}')
    if rank_airflow_com == "":
        print('airflow.com not in top-1m')
    else:
        print(rank_airflow_com)


default_args = {
    'owner': 'a-nozdrin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 29),
}
schedule_interval = '0 16 * * *'

dag = DAG('a-nozdrin_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_domain_with_max_len',
                        python_callable=get_domain_with_max_len,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow_com',
                        python_callable=get_rank_airflow_com,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)