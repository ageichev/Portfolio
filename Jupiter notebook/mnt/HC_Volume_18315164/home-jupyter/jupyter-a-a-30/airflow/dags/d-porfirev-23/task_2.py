import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from datetime import date

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_stat_top_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain = top_data_df['domain'].str.split('.').str[-1].value_counts().head(10)
    top_10_domain = top_10_domain.reset_index()
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

def get_stat_top_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    indexmax = top_data_df['domain'].str.len().idxmax()
    top_len_domain = top_data_df.query('index == @indexmax')['domain']
    with open('top_len_domain.csv', 'w') as f:
        f.write(top_len_domain.to_csv(index=False, header=False))

def get_stat_rang_air():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rang_airflow = top_data_df.query('domain ==  "airflow.com"')
    rang_airflow = pd.Series({'rank':'no data'}) if rang_airflow.empty else rang_airflow['rank'].index[0]
    with open('rang_airflow.csv', 'w') as f:
        f.write(rang_airflow.to_csv(header=False))

def print_data():
    with open('top_10_domain.csv', 'r') as f:
        all_data_1 = f.read()
    with open('top_len_domain.csv', 'r') as f:
        all_data_2 = f.read()
    with open('rang_airflow.csv', 'r') as f:
        all_data_3 = f.read()
        
    
    d = date.today()

    print(f'Top 10 domians zones for date {d}')
    print(all_data_1)

    print(f'top domain by length for date {d}')
    print(all_data_2)
    
    print(f'domian airflow.com rank for date {d}')
    print(all_data_3)

default_args = {
    'owner': 'd-porfirev-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=23),
    'start_date': datetime(2022, 8, 23),
}
schedule_interval = '0 11 * * *'

dag = DAG('d-porfirev-23_z2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_top_domain',
                    python_callable=get_stat_top_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_stat_top_len_domain',
                        python_callable=get_stat_top_len_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_stat_rang_air',
                    python_callable=get_stat_rang_air,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
