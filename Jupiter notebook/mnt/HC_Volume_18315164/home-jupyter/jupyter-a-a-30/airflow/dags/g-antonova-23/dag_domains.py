import requests
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


def get_top_domain_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, index_col=0, names=['domain'])
    df['domain_zone'] = df.domain.apply(lambda x: x.split(sep='.')[-1])
    top_10_domain_zone = df.domain_zone.value_counts().head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(header=False))


def get_longest_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, index_col=0, names=['domain'])
    df['domain_length'] = df.domain.apply(lambda x: len(x))
    name = df.query('domain_length==domain_length.max()').domain.sort_values().head(1).item()
    with open('londest_name.txt', 'w') as f:
        f.write(name)


def get_airwlow_place():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, index_col=0, names=['domain'])
    airflow_place = df.query('domain=="airflow.com"').domain
    with open('airflow_place.csv', 'w') as f:
        f.write(airflow_place.to_csv(header=False))

def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10 = f.read()
    with open('londest_name.txt', 'r') as f:
        londest_name = f.read()
    with open('airflow_place.txt', 'r') as f:
        airflow_place = f.read()
    date = ds

    print(f'Top domains zone for date {date}')
    print(top_10)

    print(f'Longest domain for date {date}')
    print(londest_name)

    print(f'Airflow place for date {date}')
    print(airflow_place)


default_args = {
    'owner': 'g-antonova-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 22),
}
schedule_interval = '0 18 * * *'

dag = DAG('g_antonova_hw', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain_zone',
                    python_callable=get_top_domain_zone,
                    dag=dag)


t3 = PythonOperator(task_id='get_longest_name',
                    python_callable=get_longest_name,
                    dag=dag)

t4 = PythonOperator(task_id='get_airwlow_place',
                    python_callable=get_airwlow_place,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >>t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
