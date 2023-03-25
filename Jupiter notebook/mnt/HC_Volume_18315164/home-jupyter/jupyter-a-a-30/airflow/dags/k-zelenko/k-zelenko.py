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


def get_top_number_doms():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_data_df_doms = top_data_df.groupby('domain_zone', as_index=False) \
                                    .agg({'domain': 'count'}) \
                                    .sort_values('domain', ascending=False) \
                                    .head(10)
    with open('get_top_number_doms.csv', 'w') as f:
        f.write(top_data_df_doms.to_csv(index=False, header=False))


def get_long_name_dom():
    long_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    long_data_df['domain_name'] = long_data_df['domain'].str.split('.').str[0]
    long_data_df['domain_len'] = long_data_df.domain_name.str.len()
    long_name = long_data_df.sort_values('domain_len', ascending=False).head(1)
    with open('get_long_name_dom.csv', 'w') as f:
        f.write(long_name.to_csv(index=False, header=False))


def get_place_airflow():
    airflow_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_place = airflow_df.query('domain == "airflow.com"')
    if airflow_place.empty == False:
        airflow_place = airflow_df['rank']
    else:
        airflow_place = 'Not this domain!' 
    with open('get_place_airflow.txt', 'w') as f:
        f.write(airflow_place)


def print_data(ds): # передаем глобальную переменную airflow
    with open('get_top_number_doms.csv', 'r') as f:
        top_number_doms = f.read()
    with open('get_long_name_dom.csv', 'r') as f:
        long_name_dom = f.read()
    with open('get_place_airflow.txt', 'r') as f:
        place_airflow = f.read()
    date = ds

    print(f'Top 10 domain zones by number of domains {date}')
    print(top_number_doms)

    print(f'Domain with the longest name {date}')
    print(long_name_dom)
    
    print(f'Place of airflow.com {date}')
    print(place_airflow)


default_args = {
    'owner': 'k-zelenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 10),
    'schedule_interval': '0 10 * * *'
}
dag = DAG('k-zelenko', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_number_doms',
                    python_callable=get_top_number_doms,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_name_dom',
                        python_callable=get_long_name_dom,
                        dag=dag)

t4 = PythonOperator(task_id='get_place_airflow',
                        python_callable=get_place_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5