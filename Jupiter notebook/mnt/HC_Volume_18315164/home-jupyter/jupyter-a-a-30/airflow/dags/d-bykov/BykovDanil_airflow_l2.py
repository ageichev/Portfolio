import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta, datetime


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


def top_10_dom():
    top_10_dom = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_dom['dom_zone'] = top_10_dom.domain.str.split('.').str[-1]
    top_10_d = top_10_dom.groupby('dom_zone', as_index=False) \
                       .agg({'domain':'count'}) \
                       .sort_values('domain', ascending=False) \
                       .head(10)
    with open('top_10_d.csv', 'w') as f:
        f.write(top_10_d.to_csv(index=False, header=False))



def longest_name():
    longest_name = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_name['name_size'] = longest_name.domain.str.len()
    longest_n = longest_name.sort_values('name_size', ascending=False).head(1).domain
    with open('longest_n.csv', 'w') as f:
        f.write(longest_n.to_csv(index=False, header=False))


def airflow_rank():
    airflow_rank = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_a = airflow_rank.query('domain == "airflow.com"')['rank']
    with open('rank_a.csv', 'w') as f:
        f.write(rank_a.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_d.csv', 'r') as f:
        top_10_d = f.read()
    with open('longest_n.csv', 'r') as f:
        longest_n = f.read()
    with open('rank_a.csv', 'r') as f:
        rank_a = f.read()    
    
    
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top_10_d)

    print(f'The longest domain for date {date}')
    print(longest_n)
    
    print(f'Airflow rank for date {date}')
    print(rank_a)



default_args = {
    'owner': 'd.bykov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 1),
}

schedule_interval = '0 12 * * *'

dag = DAG('danil_bykov_lesson2_1605', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_dom',
                    python_callable=top_10_dom,
                    dag=dag)

t3 = PythonOperator(task_id='longest_name',
                        python_callable=longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5


