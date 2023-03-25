import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def nshev_get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def nshev_top10_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    df['domain_zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_10_domain_zone = df['domain_zone'].value_counts().to_frame().reset_index().head(10)
    top_10_domain_zone.columns = ['domain_zone','domain_count']
    
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))
        
def nshev_max_len_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    df['length'] = df.domain.apply(len)
    max_lenght_domain = df[df.length  == df['length'].max()].sort_values('domain').reset_index()['domain'][0]
    
    with open('max_len.txt', 'w') as f:
        f.write(str(max_lenght_domain))
        
def nshev_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    if 'airflow.com' in list(df.domain.unique()):
        rank_airflow = df[df.domain.str.startswith('airflow.com')].reset_index()['rank'][0]
    else:
        rank_airflow = 'undefined'
    with open('airflow.txt', 'w') as f :
        f.write(str(rank_airflow))

def nshev_print_data(ds): 
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_dom_zones = f.read()
    with open('max_len.txt', 'r') as f:
        max_len = f.read()
    with open('airflow.txt', 'r') as f:
        airflow = f.read()
        
    date = ds
    
    print(f'Results for date {date}')
    
    print(f'Top domain zones:')
    print(top_10_dom_zones)

    print(f'Domain with the longest name:')
    print(max_len)
    
    print(f'Airflow rank:')
    print(airflow)


default_args = {
    'owner': 'n-shevchenko-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 21),
}
schedule_interval = '0 13 * * *'

dag = DAG('n_shevchenko_23_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='nshev_get_data',
                    python_callable=nshev_get_data,
                    dag=dag)

t2 = PythonOperator(task_id='nshev_top10_zone',
                    python_callable=nshev_top10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='nshev_max_len_domain',
                        python_callable=nshev_max_len_domain,
                        dag=dag)

t4 = PythonOperator(task_id='nshev_airflow_rank',
                        python_callable=nshev_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='nshev_print_data',
                    python_callable=nshev_print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
