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

def top_10():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_domain_zone = df.groupby('domain_zone', as_index=False).count().sort_values(by='rank', ascending=False).domain_zone.head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_domain_zone.to_csv(index=False, header=False))

def the_longes_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_len'] =df.domain.str.len().sort_values(ascending=False)
    the_longes_domain = df.sort_values(by=['domain_len', 'domain'], ascending=False).iloc[0, 1]
    with open('the_longest_domain.txt', 'w') as f:
        f.write(the_longes_domain)

def get_air():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    a = ''
    if df[df.domain == "airflow.com"].shape[0] == 0:
        a = 'Для AirFlow нет информации'
    elif df[df.domain == "airflow.com"].shape[0] != 0:
        rank = df[df.domain == 'airflow.com'].iloc[0, 0]
        a = f'Домен airflow.com находится на {rank} месте'
    with open('airflow_answer.txt', 'w') as f:
        f.write(a)

def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_domain_zone = f.read()
    with open('the_longest_domain.txt', 'r') as f:
        the_longest_domain = f.read()
    with open('airflow_answer.txt', 'r') as f:
        airflow_answer = f.read()

    print('Топ-10 доменных зон по численности доменов:')
    print(top_10_domain_zone)

    print('Домен с самым длинным именем:')
    print(the_longest_domain)

    print(airflow_answer)


default_args = {
    'owner': 'a-borsin-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 26),
}
schedule_interval = '0 12 * * *'

dag = DAG('lesson_2_borsin.av', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10',
                    python_callable=top_10,
                    dag=dag)

t3 = PythonOperator(task_id='the_longes_domain',
                        python_callable=the_longes_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_air',
                    python_callable=get_air,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

