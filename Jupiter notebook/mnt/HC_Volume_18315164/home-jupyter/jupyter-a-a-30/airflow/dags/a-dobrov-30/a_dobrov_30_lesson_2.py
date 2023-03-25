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
        
def get_top_10_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10_zones = top_data_df
    top_data_top_10_zones['zone'] = top_data_top_10_zones.domain.apply(lambda x: x.split('.')[-1])
    top_data_top_10_zones = top_data_top_10_zones.groupby('zone', as_index = False).agg(cnt = ('domain', 'count')).sort_values('cnt', ascending = False).head(10)                                            
    with open('top_data_top_10_zones.csv', 'w') as f:
        f.write(top_data_top_10_zones.to_csv(index=False, header=False))

def get_long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_long_domain = top_data_df
    top_data_long_domain['length'] = top_data_long_domain.domain.str.len()
    top_data_long_domain = top_data_long_domain.sort_values('length', ascending = False).head(1)[['domain', 'length']]
    with open('top_data_long_domain.csv', 'w') as f:
        f.write(top_data_long_domain.to_csv(index=False, header=False))

def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_rank_airflow = top_data_df.query("domain == 'airflow.com'")['rank']
    with open('top_data_rank_airflow.csv', 'w') as f:
        f.write(top_data_rank_airflow.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('top_data_long_domain.csv', 'r') as f:
        long_domain = f.read()
    with open('top_data_rank_airflow.csv', 'r') as f:
        rank_airflow = f.read()
    date = ds

    print(f'Top 10 zones for date {date}')
    print(top_10_zones)

    print(f'The longest domain for date {date}')
    print(long_domain)
    
    print(f'Rank of airflow.com for date {date}')
    print(rank_airflow)

default_args = {
'owner': 'a-dobrov-30',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2023, 2, 23),
}
schedule_interval = '0 9 * * *'

dag = DAG('a_dobrov_30_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_domain',
                        python_callable=get_long_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                    python_callable=get_rank_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5