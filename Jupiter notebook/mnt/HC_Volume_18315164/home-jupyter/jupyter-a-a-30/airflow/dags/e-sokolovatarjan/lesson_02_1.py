import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def get_top_10_domain():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain =pd.Series([x.rsplit('.',1)[1] for x in data_df.domain]).value_counts().head(10).reset_index()['index']
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False)) 
        
def get_max_domain():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len = max([len(x) for x in data_df.domain])
    max_domain = data_df[np.where(data_df.domain.str.len() == max_len,True,False)].domain.sort_values(ascending =False).head(1)
    with open('max_domain.csv', 'w') as f:
        f.write(max_domain.to_csv(index=False, header=False))
    
def get_airflow_rank():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    Airflow_rank =pd.Series(np.where(data_df[data_df['domain']=='airflow.com'].empty, 'domain is not exist', data_df[data_df['domain']=='airflow.com'].rank))
    with open('airflow_rank.csv', 'w') as f:
        f.write(Airflow_rank.to_csv(index=False, header=False))
    
def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        all_data_top10 = f.read()
    with open('max_domain.csv', 'r') as f:
        all_data_max = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow = f.read()    
    date = ds

    print(f'Top domains for date {date}')
    print(all_data_top10)

    print(f'Longest domain for date {date}')
    print(all_data_max)
    
    print(f'Airflow rank for date {date}')
    print(all_data_airflow)


default_args = {
    'owner': 'e-sokolovatarjan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 4),
}
schedule_interval = '0 13 * * *'

dag = DAG('sokolovatarjan_les2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain',
                    python_callable=get_top_10_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_domain',
                        python_callable=get_max_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5