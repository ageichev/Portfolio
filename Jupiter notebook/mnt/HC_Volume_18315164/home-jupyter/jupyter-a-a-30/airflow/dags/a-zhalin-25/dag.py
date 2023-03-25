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


def top_10_dz():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.str.split('.')[-1]
    top_10_dz = top_data_df.groupby('domain_zone', as_index=False)\
                           .agg({'domain' : 'count'})\
                           .sort_values('domain', ascending=False)\
                           .head(10)
    with open('top_10_dz.csv', 'w') as f:
        f.write(top_10_dz.to_csv(index=False, header=False))


def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.str.len()
    longest_domain = top_data_df.sort_values(['domain', 'length'], ascending = [True, False]).head(1).domain
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)

def get_airflow_rank():
     try:
        data = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
        airflow_rank = data.query('domain == "airflow.com"')['rank'].values[0]
    except:
        airflow_rank = 'No domain'
    
    with open('airflow_com.txt', 'w') as f:
        f.write(airflow_rank)
        
        
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_dz.csv', 'r') as f:
        top_10_dz = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(top_10_dz)

    print(f'The longest domain for date {date}')
    print(longest_domain)
    
    print(f'airflow.com rank for date {date}')
    print(airflow_rank)
    
default_args = {
    'owner': 'a-zhalin-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 16),
    'schedule_interval': '0 19 * * *'
}
dag = DAG('lesson02_a-zhalin-25', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_dz',
                    python_callable=top_10_dz,
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
