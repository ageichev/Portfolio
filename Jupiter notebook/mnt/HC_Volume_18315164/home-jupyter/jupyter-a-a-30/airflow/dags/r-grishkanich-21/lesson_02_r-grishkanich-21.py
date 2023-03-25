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

def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank_domain', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain_zone = top_data_df.groupby('domain_zone', as_index = False) \
                                 .agg({'rank_domain':'count'}) \
                                 .sort_values('rank_domain', ascending = False) \
                                 .head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))
        
def get_top_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank_domain', 'domain'])
    top_data_df['domain_len'] = top_data_df['domain'].str.len()
    top_len_domain = top_data_df.sort_values(['domain_len', 'domain'], ascending=[False, True]).reset_index().domain[0]
    with open('top_len_domain.txt', 'w') as f:
        f.write(str(top_len_domain))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank_domain', 'domain'])
    rank_airflow = top_data_df.query('domain == "airflow.com"').rank_domain.to_list()
    if not rank_airflow:
        rank_airflow = 'Airflow is not rated'
    else:
        rank_airflow = f"The rank of Airflow.com - {rank_airflow[0]}"
    with open('rank_airflow.txt', 'w') as f:
        f.write(str(rank_airflow))        

def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        all_data = f.read()
    with open('top_len_domain.txt', 'r') as f:
        all_data_len = f.read()
    with open('rank_airflow.txt', 'r') as f:
        all_data_name = f.read()    
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)

    print(f'The longest domain name  {date}')
    print(all_data_len)
       
    print(f'The rank of airflow.com {date}')
    print(all_data_name)

default_args = {
    'owner': 'r-grishkanich-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 2),
}
schedule_interval = '0 12 * * *'

dag = DAG('r-grishkanich-21_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_len_domain',
                        python_callable=get_top_len_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5