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
    top_data_df['zone'] = top_data_df['domain'].str.extract(r'^.*?\.(.+)$', expand=False)
    top_10_domain_zones = top_data_df.groupby('zone').agg({'domain':'count'})\
    .sort_values('domain',ascending=False)\
    .reset_index()\
    .rename(columns={'domain':'cnt_domains'})\
    .head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=True))


def get_longest():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['len'] = data_df.domain.str.len()
    domain_longest = data_df.sort_values(['len','domain'], ascending=(False,True)).head(1)
    with open('domain_longest.csv', 'w') as f:
        f.write(domain_longest.to_csv(index=False, header=True))
    
    
def get_airflow_index():            
    try:
        output_domain = top_data_df.query('domain.str.contains("airflow.com")', engine='python').head(1)
        if output_domain.empty:
            with open('airflow_index.csv', 'w') as f:
                f.write('Entry not found')
        else:
            with open('airflow_index.csv', 'w') as f:
                f.write(output_domain.to_csv(index=False, header=False))
    except:
        with open('airflow_index.csv', 'w') as f:
            f.write('Entry not found')


def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
    with open('domain_longest.csv', 'r') as f:
        domain_longest = f.read()
    with open('airflow_index.csv', 'r') as f:
        airflow_index = f.read()
    date = ds

    print(f'Top-10 by quantity domains for date {date}')
    print(top_10_domain_zones)

    print(f'Longest domain for date {date}')
    print(domain_longest)
    
    print(f'Airflow.com position for date {date}')
    print(airflow_index)

default_args = {
    'owner': 'j_didikina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 29),
}
schedule_interval = '30 13 * * *'

dag = DAG('j_didikina', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_10 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)

t2_airflow_index = PythonOperator(task_id='get_airflow_index',
                        python_callable=get_airflow_index,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_10, t2_longest, t2_airflow_index] >> t3
