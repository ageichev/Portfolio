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


def get_top():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['subdomain'] = top_data_df.domain.str.split('.').str[-1]
    top_data_top_10 = top_data_df.groupby('subdomain', as_index=False).agg(top=('subdomain', 'count')).sort_values('top', ascending=False).head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_top_length():
    top_length_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_length_df['length'] = top_length_df.domain.str.len()
    top_length = top_length_df.sort_values(['length','domain'], ascending=False).head(1)
    with open('top_length.csv', 'w') as f:
        f.write(top_length.to_csv(index=False, header=False))
        

def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = df[df.domain == "airflow.com"]["rank"]
    rank = "No rank" if airflow_rank.empty else str(airflow_rank.values[0])
    with open('airflow_rank.csv', 'w') as f:
        f.write(rank)

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_length.csv', 'r') as f:
        all_data_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(all_data)

    print(f'Top length domain for date {date}')
    print(all_data_length)
    
    print(f'Airflow.com rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'k-shirochenkov-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 17),
}
schedule_interval = '30 10 * * *'

dag = DAG('k-shirochenkov_lesson02', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top',
                    python_callable=get_top,
                    dag=dag)

t2_length = PythonOperator(task_id='get_top_length',
                        python_callable=get_top_length,
                        dag=dag)

t2_airflow_rank = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_length, t2_airflow_rank] >> t3
