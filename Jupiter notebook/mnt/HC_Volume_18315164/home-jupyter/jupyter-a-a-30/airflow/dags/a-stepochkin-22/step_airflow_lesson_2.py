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
    top_10 = df['domain'].str.split('.').apply(lambda x: x[-1]).value_counts().reset_index()['index'].head(10)
    top_10 = top_10.tolist()
    top_10 = ', '.join(top_10)
    with open('top_10.csv', 'w') as f:
        f.write(top_10)

def long_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    d = df['domain'].str.split('.').apply(lambda x: x[0])
    c = sorted(d, key=len, reverse=True)
    e = [c[0]]
    k = 1
    while len(c[k]) == len(c[0]):
        e.append(c[k])
        k += 1
    b = sorted(e)[0]
    with open('long_name.txt', 'w') as f:
        f.write(b)

def find_domain(x = 'airflow.com'):
    global c
    c=x
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if x in df.domain.values:
        fd = df[df.domain == x]['rank']
    else:
        fd = 'Нет в указанном файле'
    with open('index_domain.txt', 'w') as f:
        f.write(fd)

def print_data():
    with open('top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('long_name.txt', 'r') as f:
        long_name = f.read()
    with open('index_domain.txt', 'r') as f:
        index_domain = f.read()
    date = datetime.today().date()

    print(f'Топ-10 доменных зон {date}:')
    print(top_10)
    print()

    print(f'Домен с самым длинным именем {date}:')
    print(long_name)
    print()

    print(f'Домен с именем {c} {date} находится на месте:')
    print(index_domain)


default_args = {'owner': 'alexs-77@mail.ru','depends_on_past': False,'retries': 2,'retry_delay': timedelta(minutes=5),'start_date': datetime(2022, 7, 19)}

schedule_interval = '30 7 * * *'

dag = DAG('alexs-77@mail.ru', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data', python_callable=get_data, dag=dag)

t2_top = PythonOperator(task_id='top_10', python_callable=top_10, dag=dag)

t2_name = PythonOperator(task_id='long_name', python_callable=long_name,  dag=dag)

t2_index = PythonOperator(task_id='find_domain', python_callable=find_domain, dag=dag)

t3 = PythonOperator(task_id='print_data',  python_callable=print_data,  dag=dag)

t1 >> [t2_top, t2_name, t2_index] >> t3


