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

def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])  
    top_data_top_10 = top_data_df.groupby('domain_zone',as_index=False).domain.agg('count').sort_values('domain', ascending=False).head(10)
    
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))  

def max_len_domen():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.str.len()
    max_len = top_data_df.domain_len.max()
    max_len_domain = top_data_df.query('domain_len == @max_len').sort_values('domain',ascending=True).head(1).domain.values[0]
    max_len_domain = pd.Series(max_len_domain)
    
    with open('max_len_domain.csv', 'w') as f:
        f.write(max_len_domain.to_csv(index=False, header=False))

def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain'] = top_data_df.domain.apply(lambda x: x.lower())
    try:
        airflow_rank = top_data_df.query('domain == "airflow.com"')['rank'].values[0]
        airflow_rank = f'The rank of domain airflow.com is {airflow_rank}'
    except:
        airflow_rank = 'Domain airflow.com is not on the list'
    airflow_rank = pd.Series(airflow_rank)
        
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))   

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('max_len_domain.csv', 'r') as f:
        max_len_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zones of quantity domens for date {date}')
    print(all_data)
    
    print(f'The domain with max lenght name for date {date}')
    print(max_len_domain)
    
    print(f'{airflow_rank} for date {date}')

default_args = {
    'owner': 'm-lunin-28',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 14),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('m_lunin_28_dag', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)
t3 = PythonOperator(task_id='max_len_domen',
                    python_callable=max_len_domen,
                    dag=dag)
t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5