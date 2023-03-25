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


def get_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_zone = top_data_df.groupby(['zone'], as_index=False) \
    .agg({'domain': 'count'}) \
    .sort_values(['domain'], ascending=False) \
    .rename(columns = {'domain': 'number'}) \
    .head(10)
    with open('top_data_zone.csv', 'w') as f:
        f.write(top_data_zone.to_csv(index=False, header=False))


def get_long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['lenth'] = top_data_df['domain'].str.len()
    top_data_long = top_data_df.loc[top_data_df.lenth == top_data_df.lenth.max()] \
        .sort_values(['domain'], ascending=True) \
        .head(1).reset_index(drop=True).domain #.loc[0]
    with open('top_data_long.csv', 'w') as f:
        f.write(top_data_long.to_csv(index=False, header=False))
        
        
def get_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_rank = top_data_df.loc[top_data_df.domain == 'airflow.com']['rank'].reset_index(drop=True)
    if top_data_rank.shape[0] != 0:
        top_data_rank = top_data_rank
    else:
        top_data_rank = pd.DataFrame({'col1': ['not found']})
    with open('top_data_rank.csv', 'w') as f:
        f.write(top_data_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_zone.csv', 'r') as f:
        top_data_zone = f.read()
    with open('top_data_long.csv', 'r') as f:
        top_data_long = f.read()
    with open('top_data_rank.csv', 'r') as f:
        top_data_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_data_zone)
    print(f'')
    print(f'Domain with the longest name for date {date} is {top_data_long}')
    print(f'')
    print(f'The rank of domain airflow.com is {top_data_rank} for date {date}')


default_args = {
    'owner': 'a-kanashkina-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 24),
}
schedule_interval = '0 10 * * *'

dag = DAG('akanashkina23_top_10_ru_new', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zone',
                    python_callable=get_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_domain',
                        python_callable=get_long_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank',
                        python_callable=get_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5