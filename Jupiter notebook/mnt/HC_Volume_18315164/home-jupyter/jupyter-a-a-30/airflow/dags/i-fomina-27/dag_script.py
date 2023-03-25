import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from io import StringIO
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_doms = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    top_data = pd.read_csv(StringIO(top_doms), names=['rank', 'domain'])
    
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data.to_csv(index=False))


def get_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    zones = df['domain'].str.rpartition('.')
    zones.columns = ['site', 'sep', 'zone']
    top_10_zones = zones.groupby('zone', as_index=False).agg({'site': 'count'}).sort_values('site', ascending=False).reset_index().zone.head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))


def get_the_longest():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    site_names = df.domain.to_list()
    site_names.sort(key = lambda s: len(s), reverse=True)
    site_names[0]
    with open('site_names.txt', 'w') as f:
        f.write(site_names[0])


def get_place_of_site():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    idx = df[df['domain'] == "airflow.com"].index[0]
    with open('idx.txt', 'w') as f:
        f.write(str(idx))


def print_data():
    top_10_zones = pd.read_csv('top_10_zones.csv', names=['zone'])
    site_names = pd.read_csv('site_names.txt', names=['the_longest'])
    idx = pd.read_csv('idx.txt', names=['idx'])
        
    print(f'Топ-10 доменных зон по численности доменов (по убыванию): {top_10_zones}')

    print(f'\nДомен с самым длинным именем: {site_names.the_longest[0]}')
    
    print(f'\nДомен airflow.com находится на {idx.idx[0]} месте')


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 21),
}
schedule_interval = '0 10 * * *'

dag = DAG('fomina_domains', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zones',
                    python_callable=get_zones,
                    dag=dag)


t3 = PythonOperator(task_id='get_the_longest',
                    python_callable=get_the_longest,
                    dag=dag)


t4 = PythonOperator(task_id='get_place_of_site',
                    python_callable=get_place_of_site,
                    dag=dag)
                

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
