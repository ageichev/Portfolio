import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top10_domain_zone = top_data_df.groupby('domain_zone', as_index = False).agg({'domain':'count'}).sort_values('domain', ascending = False).head(10)
    with open('top10_domain_zone.csv', 'w') as f:
        f.write(top10_domain_zone.to_csv(index=False, header=False))


def get_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['lenth'] = top_data_df['domain'].str.len()
    max_length_domain = top_data_df.sort_values(['lenth', 'domain'], ascending = (False, True)).head(1)
    with open('max_length_domain.csv', 'w') as f:
         f.write(max_length_domain.to_csv(index=False, header=False))

def domain_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_found = top_data_df[top_data_df['domain'] == 'airflow.com']
    with open('airflow_found.csv', 'w') as f:
        f.write(airflow_found.to_csv(index=False, header=False))
        
def print_data(ds):
    with open('top10_domain_zone.csv', 'r') as f:
        all_data = f.read()
    with open('max_length_domain.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow_found.csv', 'r') as f:
        all_data_name = f.read()    
    date = ds

    print('Top-10 domains')
    print(all_data)
    print('Domain with max lenth')
    print(all_data_len)
    print('The rank of airflow.com')
    print(all_data_name)

default_args = {
    'owner': 'e-korobkova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
    'start_date': datetime(2023, 1, 25),
}
schedule_interval = '0 20 * * *'


dag = DAG('e-korobkova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2 = PythonOperator(task_id='get_top10_domain_zone',
                    python_callable=get_top10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='domain_rank',
                    python_callable=domain_rank,
                    dag=dag)

t4 = PythonOperator(task_id='get_length',
                        python_callable=get_length,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

