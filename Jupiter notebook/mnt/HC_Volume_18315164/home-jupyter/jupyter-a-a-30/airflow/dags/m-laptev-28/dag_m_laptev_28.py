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

# load data
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# get top 10 zone on count of domains
def get_top10_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    df1 = df.groupby('zone') \
            .agg({'domain': 'count'}) \
            .rename(columns={'domain': 'count'}) \
            .sort_values('count', ascending=False) \
            .reset_index() \
            .head(10)
    with open('top10_zone_count.csv', 'w') as f:
        f.write(df1.to_csv(index=False))

# get lohgest name of domain
def get_max_length():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['name_length'] = df.domain.apply(len)
    max_length = df.name_length.max()
    max_length_name = df.query('name_length == @max_length').sort_values('domain', ascending=False).iloc[0][1]
    with open('max_length_domain_name.txt', 'w') as f:
        f.write(max_length_name)

# get rank of airflow.com
def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']).query('domain == "airflow.com"')
    message = f'airflow.com not presented in domain list' if df.empty else f'airflow.com rank is {df.iloc[0][0]}'
    with open('airflow_rank.txt', 'w') as f:
        f.write(message)

# output data
def print_data(ds):
    with open('top10_zone_count.csv', 'r') as f:
        top10_zones = f.read()
    with open('max_length_domain_name.txt', 'r') as f:
        max_length_name = f.read()
    with open('airflow_rank.txt', 'r') as f:
        message = f.read()
    date_ = ds

    print(f'Top 10 zones on domains count for date {date_}')
    print(top10_zones)
    print(f'Max length name of domain for date {date_}')
    print(max_length_name)
    print(f'{message} for date {date_}')


default_args = {
    'owner': 'm.laptev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 16),
    'schedule_interval': '0 12 * * *'
}

dag = DAG('m_laptev_28_less2dag', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_zone',
                    python_callable=get_top10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length',
                    python_callable=get_max_length,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
