import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data() :
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index = False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f :
        f.write(top_data)

def get_top_10_domain_zones() :
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    df['dz'] = df['domain'].str.extract(r"\w*[.]([a-zA-Z]{1,3})")
    top_10_domain_zones = df.groupby('dz') \
                            .agg({'domain': 'count'}) \
                            .sort_values(by = 'domain', ascending = False) \
                            .head(10)

    with open('top_10_domain_zones.csv', 'w') as f :
        f.write(top_10_domain_zones.to_csv(header = False))

def get_longest_name() :
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    df['length'] = df['domain'].str.len()
    longest_name = df.sort_values(by = ['length', 'domain'], \
                     ascending = [False, True]) \
                    .head(1) \
                    ['domain'].item()

    with open('longest_name.txt', 'w') as f :
        f.write(longest_name)

def get_rank_of_airflow() :
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    airflow_rank = df[df['domain'].str.fullmatch('airflow.com', case = False)].index.to_list()
    airflow_rank = '' if (airflow_rank == []) else airflow_rank[0]

    with open('rank_of_airflow.txt', 'w') as f :
        f.write("{}".format(airflow_rank))

def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f :
        top_10_dz = f.read()
    with open('longest_name.txt', 'r') as f :
        longest_name = f.read()
    with open('rank_of_airflow.txt', 'r') as f :
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_dz)

    print(f'Longest domain name for date {date}')
    print(longest_name)

    print(f'Rank of airflow.com for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'ay-khaktynova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5),
    'start_date': datetime(2022, 10, 20),
}
schedule_interval = '0 12 * * *'

dag = DAG('ay-khaktynova',
          default_args = default_args,
          schedule_interval = schedule_interval)


t1 = PythonOperator(task_id = 'get_data',
                    python_callable = get_data,
                    dag = dag)

t2 = PythonOperator(task_id = 'get_top_10_domain_zones',
                    python_callable = get_top_10_domain_zones,
                    dag = dag)

t3 = PythonOperator(task_id = 'get_longest_name',
                    python_callable = get_longest_name,
                    dag = dag)

t4 = PythonOperator(task_id = 'get_rank_of_airflow',
                    python_callable = get_rank_of_airflow,
                    dag = dag)

t5 = PythonOperator(task_id = 'print_data',
                    python_callable =  print_data,
                    dag = dag)

t1 >> [t2, t3, t4] >> t5




