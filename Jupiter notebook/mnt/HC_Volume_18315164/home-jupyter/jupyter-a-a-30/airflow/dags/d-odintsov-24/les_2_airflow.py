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

def top_10():
    df_1 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain'])
    df_1['domain_zone'] = df_1.domain.apply(lambda x: x.split('.')[-1])
    top_domains = df_1.groupby('domain_zone', as_index=False)\
                      .count().sort_values('domain', ascending=False)\
                      .head(10)
    with open('top_10.csv', 'w') as f:
        f.write(top_domains.to_csv(index=False, header=False))

def longest_domain():
    df_2 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain'])
    df_2['true_domain'] = df_2.domain.apply(lambda x: x.split('.')[-2]) #колонка с доменами (без доменной зоны)
    df_2['domain_length'] = df_2.true_domain.apply(lambda x: len(x)) #длина доменов
    max_length = df_2.domain_length.max() 
    top = sorted(df_2.query('domain_length == @max_length').true_domain.to_list())
    with open('longest_domain.txt', 'w') as f:
        f.write(top[0]) 

def rank():
    df_3 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain'])
    try:
        rank = df_3.loc[df_3['domain'] == "airflow.com"].index.item() #смотрим входит ли домен airflow.com в топ 1м 
    except:
        rank = 'Airflow.com is not in 1M most popular domains now' #если не входит, то выводим сообщение об этом
    with open('rank.txt', 'w') as f:
        f.write(rank) 

def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top_10_df = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain_df = f.read()
    with open('rank.txt', 'r') as f:
        rank_df = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top_10_df)

    print(f'Longest domain for date {date}')
    print(longest_domain_df)

    print(f'Airflow rank for date {date}')
    print(rank_df)

default_args = {
    'owner': 'd-odintsov-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 9, 16),
}
schedule_interval = '0 15 * * *'

dag = DAG('lesson_2_d-odintsov-24', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10',
                    python_callable=top_10,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                    python_callable=longest_domain,
                    dag=dag)

t4 = PythonOperator(task_id='airlow_rank',
                    python_callable=rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
