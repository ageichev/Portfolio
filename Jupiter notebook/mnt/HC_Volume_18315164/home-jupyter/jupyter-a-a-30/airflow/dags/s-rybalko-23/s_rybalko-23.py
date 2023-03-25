import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone_name'] = top_data_df['domain'].str.split('.').str[-1]
    top_domain_zones = top_data_df.groupby('zone_name', as_index=False).domain.count().sort_values('domain', ascending=False).head(10)
    with open('top_domain_zones.csv', 'w') as f:
        f.write(top_domain_zones.to_csv(index=False, header=False))


def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df['domain'].str.len()
    longest_domain = top_data_df.sort_values(['len_domain','domain'], ascending=[False,True]).head(1)['domain'].values[0]
    with open('longest_domain.txt', 'w') as f:
        f.write(str(longest_domain))
    
       

def domain_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
   
    try:
        domain_airflow = top_data_df[top_data_df['domain'] == "airflow.com"]['rank'].values[0]
    except:
        domain_airflow = 'Домена airflow.com нет в списке'
    
    with open('domain_airflow.txt', 'w') as f:
        f.write(str(domain_airflow))
        

def print_data(ds):
    with open('top_domain_zones.csv', 'r') as f:
        top_domain_zones = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
    with open('domain_airflow.txt', 'r') as f:
        domain_airflow = f.read()
    date = ds
    
    print(f'Top 10 domain zones for date {date}')
    print(top_domain_zones)

    print(f'The domain with the longest name for date {date}')
    print(longest_domain)

    print(f'rank domain airflow.com for date {date}')
    print(domain_airflow)

default_args = {
    'owner': 's.rybalko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 22),
}
schedule_interval = '0 10 * * *'

dag = DAG('s.rybalko_23', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_domain_zones',
                    python_callable=top_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='domain_airflow',
                    python_callable=domain_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5
