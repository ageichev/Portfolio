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


def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df.domain.str.split('.').str[-1].value_counts()
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.domain.str.split('.').str[0].str.len()
    top_data_max_domain = top_data_df.sort_values('domain_length', ascending = True).reset_index().head(1)
    with open('top_data_max_domain.csv', 'w') as f:
        f.write(top_data_max_domain.to_csv(index=False, header=False))

        
def get_index():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_index = top_data_df.domain.str.split('.').str[-2].reset_index()
    top_data_index = top_data_index.loc[top_data_index.domain == 'airflow']
    with open('top_data_index.csv', 'w') as f:
        f.write(top_data_index.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_data = f.read()
    with open('top_data_max_domain.csv', 'r') as f:
        max_length_domain = f.read()
    with open('top_data_index.csv', 'r') as f:
        airflow_index = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)

    print(f'Top length domain for date {date}')
    print(max_length_domain)

    print(f'Index for airflow.com for date {date}')
    print(airflow_index)
    
default_args = {
    'owner': 'e.servetnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 10),
}
schedule_interval = '0 10 * * *'

dag = DAG('top_10', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_domain',
                    python_callable=get_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_index',
                        python_callable=get_index,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
