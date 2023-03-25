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


def top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df['domain'].str.split('.').apply(lambda x: x[-1]).value_counts() \
                                                        .reset_index()[['index']].head(10).rename(columns={'index': 'domain'})
    with open('top_data_top_10.csv', 'w') as f:
            f.write(top_data_top_10.to_csv(index=False, header=False))
    
def long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.apply(lambda x: len(x))
    top_data_df_len = top_data_df.sort_values(['len', 'domain'], ascending = [False, True])[['domain']].head(1)
    with open('top_data_len.csv', 'w') as f:
            f.write(top_data_df_len.to_csv(index=False, header=False))
    
def get_position_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_domain = top_data_df[top_data_df.domain.str.endswith('airflow.com')][['rank']]
    with open('top_data_domain.csv', 'w') as f:
        f.write(top_data_domain.to_csv(index=False, header=False)) 


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_data_top_10 = f.read()
    with open('top_data_len.csv', 'r') as f:
        top_data_len = f.read()
    with open('top_data_domain.csv', 'r') as f:
        top_data_domain = f.read()
    date = ds

    print(f'Top domains zone for date {date}')
    print(top_data_top_10)

    print(f'Top domains len for date {date}')
    print(top_data_len)
    
    print(f'Airflow rank for date {date}')
    print(top_data_domain)


default_args = {
    'owner': 'v-korepanov-27',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2022, 12, 30),
    'schedule_interval': '30 18 * * *'
}

dag = DAG('v-korepanov-27_top_10_ru_new', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domains',
                    python_callable=top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='long_domain',
                        python_callable=long_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_position_airflow',
                    python_callable=get_position_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5