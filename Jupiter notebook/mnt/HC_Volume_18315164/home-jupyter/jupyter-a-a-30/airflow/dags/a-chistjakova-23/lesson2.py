import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_domain_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain = df.groupby('zone', as_index=False) \
        .agg({'domain': 'count'}) \
        .sort_values('domain', ascending=False) \
        .head(10)

    with open('top_10.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

def get_longest_name_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_length'] = df['domain'].str.len()
    domain_length = df.sort_values('domain_length', ascending=False) \
        .head(1) \
        .domain

    with open('longest_name.csv', 'w') as f:
        f.write(domain_length.to_csv(index=False, header=False))


def get_airflow_rank():
    airflow_rank = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) \
        .query("domain.str.contains('airflow.com')")['rank']

    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top_10 = f.read()

    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()

    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()

    print(f'топ-10 доменных зон по численности доменов {ds}: {top_10}')
    print(f'домен с самым длинным именем {ds}: {longest_name}')
    print(f'Позиция Airflow.com в общей ранке {ds}: {airflow_rank}')

default_args = {
    'owner': 'a-chistjakova-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 18),
}
schedule_interval = '0 12 * * *'
dag_anna = DAG('lesson2_anna_chistjakova',
                        default_args=default_args,
                        schedule_interval=schedule_interval,
                        tags=["lesson_2", "a-chistjakova-23"])

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_anna)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag_anna)

t3 = PythonOperator(task_id='get_longest_len_domain',
                    python_callable=get_longest_name_domain,
                    dag=dag_anna)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag_anna)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_anna)

t1 >> [t2, t3, t4] >> t5
