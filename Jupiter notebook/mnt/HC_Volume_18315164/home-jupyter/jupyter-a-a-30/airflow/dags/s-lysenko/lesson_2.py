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

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_domain_zone_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = '.' + top_data_df.domain.str.split('.').str[-1]
    domain_zone_top_10 = top_data_df.domain_zone.value_counts().head(10)
    with open('domain_zone_top_10.csv', 'w') as f:
        f.write(domain_zone_top_10.to_csv(index=True, header=False))

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.str.len()
    longest_domain = top_data_df.sort_values(['len', 'domain'], ascending = [False, True]).head(1)['domain']
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))
        
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df.domain == 'airflow.com']['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('domain_zone_top_10.csv', 'r') as f:
        domain_zone_top_10 = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(domain_zone_top_10)

    print(f'Longest domain for date {date}')
    print(longest_domain)
    
    print(f'Rank of airflor.com for date {date}')
    print(airflow_rank)

    
default_args = {
    'owner': 's-lysenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 9),
    'schedule_interval': '30 18 * * *'
}
dag = DAG('s-lysenko_lesson_2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_zone_top_10',
                    python_callable=get_domain_zone_top_10,
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