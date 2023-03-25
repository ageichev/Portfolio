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
        
def top_10_domain_zone():
    top_10_domain_zone = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain_zone['domain_zone'] = top_10_domain_zone.domain.str.split('.').str[-1]    
    top_10_domain = top_10_domain_zone.domain_zone.value_counts().sort_values(ascending=False).reset_index().head(10)
    top_10_domain = top_10_domain.rename(columns = {'index' : 'domain_zone', 'domain_zone':'count'})
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

def max_length_domain():
    max_len_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len_domain['domain_length'] = max_len_domain['domain'].apply(lambda x: len(x))
    max_len_domain = max_len_domain.sort_values(['domain_length', 'domain'], ascending=[False, True]).iloc[0].domain
    with open('max_len_domain.txt', 'w') as f:
        f.write(str(max_len_domain))
        
def airflow_rank():
    airflow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if 'airflow.com' in airflow['domain']:
        rank = airflow.query('domain == "airflow.com"')['rank']
    else:
        rank = 'there is no airflow.com domain'
    with open('result.txt', 'w') as f:
        f.write(str(rank))      
        
def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        top_10_domain = f.read()
    with open('max_len_domain.txt', 'r') as f:
        max_len_domain = f.read()
    with open('rank.txt', 'r') as f:
        rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}:')
    print(top_10_domain)

    print(f'The longest domain is:')
    print(max_len_domain)

    print('airflow.com domain rank is:')
    print(rank)     
    
default_args = {
    'owner': 'a-matveeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 1),
    'schedule_interval': '0 9 * * *'
}

dag = DAG('a-matveeva_lesson_2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zone',
                    python_callable=top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='max_length_domain',
                    python_callable=max_length_domain,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5