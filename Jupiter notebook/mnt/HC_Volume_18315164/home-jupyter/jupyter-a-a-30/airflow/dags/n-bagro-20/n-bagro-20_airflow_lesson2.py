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

def get_data_domains():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def get_domain_zone():
    rank_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_domain_df['domain_zone'] = rank_domain_df.domain.apply(lambda x: x.split('.')[1])
    domain_zone_top_10 = rank_domain_df.value_counts('domain_zone').head(10)
    with open('domain_zone_top_10.csv', 'w') as f:
        f.write(domain_zone_top_10.to_csv(index=True, header=False))  
        
def get_len_domain_name():
    len_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    len_domain_df['len_domain'] = len_domain_df.domain.apply(lambda x: len(x))
    len_max = len_domain_df.domain.str.len().max()
    len_domain_name = len_domain_df.query('len_domain == @len_max').domain.sort_values().iloc[0]
    with open('len_domain_name.txt', 'w') as f:
        f.write(len_domain_name)
        
def get_website():
    rank_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_domain_airflow = rank_domain_df.query('domain == "airflow.com"')
    with open('rank_domain_airflow.csv', 'w') as f:
        f.write(rank_domain_airflow.to_csv(index=False, header=False))
        
def print_data_domeins(ds): # передаем глобальную переменную airflow
    with open('domain_zone_top_10.csv', 'r') as f:
        all_data_domain_top = f.read()
    date = ds
    print(f'Top domain zones for date {date}')
    print(all_data_domain_top)
    
    with open('len_domain_name.txt', 'r') as f:
        all_data_len_domain = f.read()
    print(f'Longest domain name for date {date}')
    print(all_data_len_domain)
    
    with open('rank_domain_airflow.csv', 'r') as f:
        all_data = f.read()
    date = ds   
    if all_data == '':
        print(f'\nDomain airflow.com for date {date} not in TOP_1M_DOMAINS')
    else:
        print(f'\nRank for airflow.com for date {date}', all_data)
        
default_args = {
    'owner': 'n-bagro-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 4),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('n-bagro-20_data_domeins', default_args=default_args)   

t1 = PythonOperator(task_id='get_data_domains',
                    python_callable=get_data_domains,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_zone',
                    python_callable=get_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_len_domain_name',
                        python_callable=get_len_domain_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_website',
                    python_callable=get_website,
                    dag=dag)

t5 = PythonOperator(task_id='print_data_domeins',
                    python_callable=print_data_domeins,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5