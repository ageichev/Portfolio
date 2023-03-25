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
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_domains():
    top_domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domains.domain = top_domains.domain.apply(lambda x: x.split('.')[-1])
    top_domains = top_domains \
        .groupby('domain', as_index=False) \
        .agg({'rank' : 'count'}) \
        .sort_values('rank', ascending=False) \
        .reset_index(drop=True) \
        .head(10)
    with open('top_domains.csv', 'w') as f:
        f.write(top_domains.to_csv(index=False, header=False))

        
def get_longest_dom():
    longest_dom = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_dom['name_length'] = longest_dom.domain.apply(lambda x: len(x))
    longest_dom = longest_dom.sort_values('name_length', ascending=False).head(1)
    with open('longest_dom.csv', 'w') as f:
        f.write(longest_dom.to_csv(index=False, header=False))

        
def get_airflow_rank():
    domains_data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = domains_data[domains_data.domain == 'airflow.com']
    print(airflow_rank)
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_domains.csv', 'r') as f:
        top_domains_data = f.read()
    with open('longest_dom.csv', 'r') as f:
        longest_dom_data = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()
    date = ds

    print(f'Top domain zones by number of domains for date {date}')
    print(top_domains_data)

    print(f'Top longest domain for date {date}')
    print(longest_dom_data)
    
    print(f'Airflow.com rank for date {date}')
    print(airflow_rank_data)


default_args = {
    'owner': 'a-mihajlov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 13),
}
schedule_interval = '0 12 * * *'

dag_a_mihajlov = DAG('domains_info', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_a_mihajlov)

t2 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag_a_mihajlov)

t2_length = PythonOperator(task_id='get_longest_dom',
                        python_callable=get_longest_dom,
                        dag=dag_a_mihajlov)

t2_aflow = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag_a_mihajlov)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_a_mihajlov)

t1 >> [t2, t2_length, t2_aflow] >> t3
