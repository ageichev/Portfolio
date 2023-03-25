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

def get_stat_top_10_by_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df[['domain', 'zone']] = top_data_df['domain'].str.split('.', 1, expand=True)
    top_10_by_zone = top_data_df.groupby('zone', as_index = False) \
        .agg({'domain':'count'}) \
        .sort_values('domain', ascending = False) \
        .head(10)
    with open('top_10_by_zone.csv', 'w') as f:
        f.write(top_10_by_zone.to_csv(index=False, header=False))



def get_long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['count_symbols'] = top_data_df['domain'].apply(len)
    long_domain = top_data_df.sort_values('count_symbols', ascending=False).head(1)
    with open('long_domain.csv', 'w') as f:
        f.write(long_domain.to_csv(index=False, header=False))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(): # передаем глобальную переменную airflow
    with open('top_10_by_zone.csv', 'r') as f:
        all_data = f.read()

    with open('long_domain.csv', 'r') as f:
        all_data_long = f.read()

    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow = f.read()

    print(f'Top 10 domains by zone')
    print(all_data)

    print(f'Longest domain')
    print(all_data_long)

    print(f'Airflow.com rank')
    print(all_data_airflow)


default_args = {
    'owner': 'd.ermolaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 6),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('lesson_2_d.ermolaev', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_top_10_by_zone',
                    python_callable=get_stat_top_10_by_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_domain',
                        python_callable=get_long_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5