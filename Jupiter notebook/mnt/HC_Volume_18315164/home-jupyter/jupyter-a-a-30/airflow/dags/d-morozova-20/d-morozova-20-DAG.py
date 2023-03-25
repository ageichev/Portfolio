import numpy as np
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


def get_top10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_1'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_domains_list = list(top_data_df['domain_1'].value_counts().index[:10])
    with open('top10.txt', 'w') as f:
        f.write(str(top_domains_list))


def get_longest_domain_and_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].str.len()
    top_data_df = top_data_df.sort_values(['domain_length', 'domain'], ascending=[False, True])
    longest_domain = top_data_df.iloc[0]['domain']
    with open('longest_domain.txt', 'w') as f:
        f.write(str(longest_domain))
    domain_name = 'airflow.com'
    airflow_rank = -1
    if domain_name in top_data_df['domain'].unique():
        airflow_rank = np.where(top_data_df["domain"] == domain_name)[0][0]
    with open('airflow_rank.txt', 'w') as f:
        f.write(str(airflow_rank))       


def print_data():
    with open('top10.txt', 'r') as f:
        top_domains_list = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read() 
        
    date = datetime.today()
    print(f'Top 10 domain zones for date {date.strftime("%d.%m.%Y")}: ')
    print(top_domains_list)
    print('---------')
    print(f'The longest domain for date {date.strftime("%d.%m.%Y")}:')
    print(longest_domain)
    print('---------')    
    print(f'Rank of airflow.com domain for date {date.strftime("%d.%m.%Y")}:')
    if airflow_rank == '-1':
        print('No airflow.com domain in the list')
    else:
        print(airflow_rank)


default_args = {
    'owner': 'd.morozova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 29),
}
schedule_interval = '30 13 * * *'

dag = DAG('d-morozova-20-DAG', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top10',
                    python_callable=get_top10,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_longest_domain_and_airflow_rank',
                        python_callable=get_longest_domain_and_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2] >> t3
