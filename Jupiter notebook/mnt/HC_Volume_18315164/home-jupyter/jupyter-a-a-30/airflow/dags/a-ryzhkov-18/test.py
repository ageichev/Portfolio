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

def count_domaine_zone(c):
    x = c.split('.')[1:]
    return '.'.join(x)

def get_stat_top_10_domaine_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.apply(count_domaine_zone)
    top_10_domaine_zone = top_data_df.zone.value_counts().head(10)
    with open('top_10_domaine_zone.csv', 'w') as f:
        f.write(top_10_domaine_zone.to_csv(index=False, header=False))


def get_stat_len_domaine():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_dom'] = top_data_df.domain.apply(lambda x: len(x))
    max_len = top_data_df.len_dom.max()
    max_len_df = top_data_df.query('len_dom == @max_len')
    lst_len = list(max_len_df.domain.unique())
    if len(lst_len) > 2:
        lst = lst_len.sort()
        lst_len = lst_len[0]
    with open('len_domaine.csv', 'w') as f:
        f.write(lst_len.to_csv(index=False, header=False))


def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_df = top_data_df.query('domain == "airflow.com"')
    with open('airflow_df.csv', 'w') as f:
        f.write(airflow_df.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domaine_zone.csv', 'r') as f:
        all_data_zone = f.read()
    with open('lst_len.csv', 'r') as f:
        lst_len_name = f.read()
    with open('airflow_df.csv', 'r') as f:
        airflow_df_done = f.read()
    date = ds

    print(f'Top zone domains for date {date}')
    print(all_data_zone)

    print(f'Max len domain  for date {date}')
    print(lst_len_name)

    print(f'Rank domain "airflow.com"  for date {date}')
    print(airflow_df_done)


default_args = {
    'owner': 'a.ryzhkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 23),
    'schedule_interval': '30 * * * *'
}
dag = DAG('first_dag_1233-58', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_top_10_domaine_zone',
                    python_callable=get_stat_top_10_domaine_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_stat_len_domaine',
                        python_callable=get_stat_len_domaine,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5