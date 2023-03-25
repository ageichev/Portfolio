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
        
def get_top_zones(ds):
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_zones = top_data_df.groupby('zone') \
                            .agg({'domain': 'count'}) \
                            .sort_values(by='domain', ascending=False) \
                            .head(10) \
                            .index \
                            .to_list()
    
    print(f'Top-10 domain zones for {ds}: ', end='')
    print(*top_zones, sep=', ')

def get_longest_domain(ds):
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_len'] = df.domain.apply(lambda x: len(x))
    name = df.sort_values(by='domain_len', ascending=False).head(1).iloc[0]['domain']
    
    print(f'The longest domain name for {ds} is: ')
    print(name)

def get_airflow_rank(ds):
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    r = df.index[df['domain'] == 'airflow.com'].tolist()
    
    if len(r) == 0:
        print(f'The rank of the Airflow.com for {ds} is missing')
    else:
        print(f'The rank of the Airflow.com for {ds} is {r[0]}')

default_args = {
    'owner': 'd-gorbunov-22',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 24),
}
schedule_interval = '0 10 * * *'

dag = DAG('dgorbunov22-lesson2', default_args=default_args, schedule_interval=schedule_interval)

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

t1 >> [t2, t3, t4]