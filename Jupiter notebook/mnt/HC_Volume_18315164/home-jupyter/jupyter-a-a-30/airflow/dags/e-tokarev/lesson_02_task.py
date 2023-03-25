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

# топ-10 доменных зон по численности доменов
def get_top_10_dzones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain_zones = top_data_df\
        .groupby(top_data_df['domain'].str.split('.').str[-1])\
        .aggregate({'domain' : 'count'}, ascending=False)\
        .rename({'domain':'counts'}, axis=1)\
        .sort_values(by='counts', ascending=False)\
        .reset_index().head(10)

    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))

# домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    get_longest_domain = top_data_df
    get_longest_domain['ln'] = top_data_df.domain.str.len()
    maxln = get_longest_domain.domain.str.len().max()
    longest_domain = get_longest_domain.query(f'ln == {maxln}').domain.sort_values().to_list()[0]

    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain)

# место домена airflow.com
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank = top_data_df.query('domain == "airflow.com"')['rank'].to_list()
    if rank == []:
        rank = 'airflow.com has no rank'
    else:
        rank = f'airflow.com has {rank[0]} rank'

    with open('rank.csv', 'w') as f:
        f.write(rank)

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domain_zones.csv', 'r') as f:
        top_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('rank.csv', 'r') as f:
        rank = f.read()
    date = ds

    print(f'Top domain zones by count for date {date}: ')
    print(top_zones)

    print(f'longest_domain for date {date}: ')
    print(longest_domain)

    print(f'{rank} for date {date}')

default_args = {
    'owner': 'e.tokarev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 11),
    'schedule_interval': '0 24 * * *'
}
dag = DAG('e-tokarev_lesson_2_task', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_dzones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5