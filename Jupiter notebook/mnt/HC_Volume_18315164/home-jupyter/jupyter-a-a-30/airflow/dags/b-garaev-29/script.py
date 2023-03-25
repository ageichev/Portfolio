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
        
def get_top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domains = top_data_df.domain.apply(lambda x: x.split('.')[-1]).value_counts()
    top_10_domains = top_dom.head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(header=False))
        
def get_max_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len = len(max(top_data_df.domain, key=len))
    top_data_df['len_dom'] = top_data_df.domain.apply(lambda x: len(x))
    top_len_domain = top_data_df.query('len_dom == @max_len').sort_values(by='domain')['domain'].head(1)
    with open('top_len_domain.csv', 'w') as f:
        f.write(top_len_domain.to_csv(index=False, header=False))
        
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domains.csv', 'r') as f:
        all_data_10_domains = f.read()
    with open('top_len_domain.csv', 'r') as f:
        all_data_top_len = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(all_data_10_domains)

    print(f'Top length domain for date {date}')
    print(all_data_top_len)
    
    print(f'airflow rank is {all_data_airflow_rank} for date {date}')

default_args = {
    'owner': 'b-garaev-29',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 9),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('b-garaev-29', default_args=default_args)
    
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len_domain',
                        python_callable=get_max_len_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag) 

t1 >> [t2, t3, t4] >> t5