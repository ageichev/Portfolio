import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['core_domain'] =top_data_df.domain.str.split('.',1)
    top_data_df['core_domain'] = top_data_df['core_domain'].str[1]
    top_data_df = top_data_df.groupby('core_domain', as_index=False)\
    .agg({'domain':'count'})\
        .sort_values('domain', ascending=False).head(10)
    with open('top_data_top_10_domain.csv', 'w') as f:
        f.write(top_data_df.to_csv(index=False, header=False))

def get_long_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.str.len()
    top_data_df = top_data_df.sort_values(['length','domain'], ascending=False).head(1)
    with open('top_long_domain_name.csv', 'w') as f:
        f.write(top_data_df.to_csv(index=False, header=False))

def get_airflow_place():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df = top_data_df[top_data_df.domain == 'airflow.com']
    place = ''
    if top_data_df.shape[0] == 0:
        place = 'Not Founded'
    else:
        place = top_data_df.loc[0,0]
    with open('airflow_place.csv', 'w') as f:
        f.write(place)


def print_data(ds):
    with open('top_data_top_10_domain.csv', 'r') as f:
        top_10 = f.read()
    with open('top_long_domain_name.csv', 'r') as f:
        long_domain_name = f.read()
    with open('airflow_place.csv', 'r') as f:
        airflow_place = f.read()
    date = ds

    print(f'Top 10 domains zones for date {date}')
    print(top_10)

    print(f'The longest domain name for date {date}')
    print(long_domain_name)

    print(f'The place of airflow.com damain for date {date}')
    print(airflow_place)


default_args = {
    'owner': 'a-rudneva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 25),
}
schedule_interval = '0 10 * * *'

dag = DAG('top_10_ru_new_a_rudneva', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_10 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t2_long_domain_name = PythonOperator(task_id='get_long_domain_name',
                        python_callable=get_long_domain_name,
                        dag=dag)

t2_airflow_place = PythonOperator(task_id='get_airflow_place',
                        python_callable=get_airflow_place,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_10, t2_long_domain_name, t2_airflow_place] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)