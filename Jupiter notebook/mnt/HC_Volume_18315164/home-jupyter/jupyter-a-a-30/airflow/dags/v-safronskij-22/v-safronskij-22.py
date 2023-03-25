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

def get_top_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domains'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_data_df_top_10 = top_data_df.groupby('domains', as_index = False).agg({'rank' : 'count'}).rename(columns={'domains' : 'domain', 'rank' : 'count'}).sort_values('count', ascending = False).head(10)
    with open('top_data_df_top_10.csv', 'w') as f:
        f.write(top_data_df_top_10.to_csv(index=False, header=False))

def get_long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: len(x.split('.')[0]))
    longest_domain = top_data_df.sort_values('domain_len', ascending = False).drop_duplicates('domain_len').head(1)[['domain']]
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

def get_airflow_index():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_index = top_data_df.query('domain == "airflow.app"')
    with open('airflow_index.csv', 'w') as f:
        f.write(airflow_index.to_csv(index=False, header=False))

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_df_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        all_data_long_domain = f.read()
    with open('airflow_index.csv', 'r') as f:
        all_data_airflow_index = f.read()
    date = ds

    print(f'Top domains for date {date}')
    print(all_data)

    print(f'longest_domain for date {date}')
    print(all_data_long_domain)

    print(f'airflow_index for date {date}')
    print(all_data_airflow_index)

default_args = {
    'owner': 'v-safronskij-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
    'schedule_interval': '0 1 * * *'
}
dag = DAG('v-safronskij-22_dag', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_domain',
                        python_callable=get_long_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_index',
                        python_callable=get_airflow_index,
                        dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> t2 >> t3 >> t4 >> t5