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
site = 'airflow.com'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_10_zone_def():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.str.split('.', n=1, expand=True).rename(columns={1:'zone'}).zone
    top_10_zone = top_data_df.groupby('domain_zone', as_index=False).agg({'domain':'count'}).sort_values('domain', ascending=False).head(10)
    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10_zone.to_csv(index=False, header=False))

def long_domain_def():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: len(x))
    long_domain = top_data_df.query("domain_len == @top_data_df.domain_len.max()").sort_values('domain').head(1)
    with open('long_domain.csv', 'w') as f:
        f.write(long_domain.domain.to_csv(index=False, header=False))

def site_rank_def():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    site_rank = top_data_df[top_data_df['domain'] == site]
    if site_rank.shape[0] == 0:
        d = {'rank': [f'Domain {site} not faund']}
        site_rank = pd.DataFrame(data=d)
    else:
        site_rank = site_rank['rank']

    with open('site_rank.csv', 'w') as f:
        f.write(site_rank.to_csv(index=False, header=False))

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_zone.csv', 'r') as f:
        all_data_zone = f.read()
    with open('long_domain.csv', 'r') as f:
        all_data_long = f.read()
    with open('site_rank.csv', 'r') as f:
        all_data_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for quantity on {date}:')
    print(all_data_zone)

    print(f'Longest domain name on {date}:')
    print(all_data_long)

    print(f'Domen {site} rank on {date}:')
    print(all_data_rank)


default_args = {
    'owner': 'p-kievskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 14),
    'schedule_interval': '0 15 * * *'
}
dag = DAG('p-kievskij', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_zone',
                    python_callable=top_10_zone_def,
                    dag=dag)

t3 = PythonOperator(task_id='long_domain',
                        python_callable=long_domain_def,
                        dag=dag)

t4 = PythonOperator(task_id='domain_rank',
                        python_callable=site_rank_def,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5