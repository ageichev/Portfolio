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


def get_stat_code():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['code'] = top_data_df['domain'].apply(lambda x: x[::-1].split('.')[0][::-1])
    top_data_code_10 = top_data_df.groupby('code', as_index=False)\
        .agg({'domain': 'count'})\
        .sort_values('domain', ascending=False).head(10)
    with open('top_data_code_10.csv', 'w') as f:
        f.write(top_data_code_10.to_csv(index=False, header=False))


def get_stat_max_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['code'] = top_data_df['domain'].apply(lambda x: x[::-1].split('.')[0][::-1])
    top_data_df['len_domain'] = top_data_df['domain'].apply(len) - top_data_df['code'].apply(len)
    top_data_max_len = top_data_df[top_data_df['len_domain'] == top_data_df['len_domain'].max()]
    with open('top_data_max_len.csv', 'w') as f:
        f.write(top_data_max_len.to_csv(index=False, header=False))

def get_stat_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    get_stat_airflow = top_data_df [top_data_df['domain'].str.startswith('airflow')]
    with open('get_stat_airflow.csv', 'w') as f:
        f.write(get_stat_airflow.to_csv(index=False, header=False))



def print_data(ds): # передаем глобальную переменную airflow
    with open('get_stat_code.csv', 'r') as f:
        all_data = f.read()
    with open('get_stat_max_len.csv', 'r') as f:
        all_data_len = f.read()
    with open('get_stat_airflow.csv', 'r') as f:
        all_data_air = f.read()
    date = ds

    print(f'Top domains code in for date {date}')
    print(all_data)

    print(f'Maximum length domain code for date {date}')
    print(all_data_len)

    print(f'Domains airflow for date {date}')
    print(all_data_air)




default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('top_10_ru_new', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_code',
                    python_callable=get_stat_code,
                    dag=dag)

t2_max_len = PythonOperator(task_id='get_stat_max_len',
                        python_callable=get_stat_max_len,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_stat_airflow',
                        python_callable=get_stat_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_max_len, t2_airflow] >> t3

