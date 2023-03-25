import requests
import pandas as pd
import numpy as np
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


def get_stat_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10_zones = top_data_df['domain'].str.split('.',expand=True)
    top_data_top_10_zones.columns =['domain_name','domain_zone','subdomain_1','subdomain_2','subdomain_3']
    top_data_top_10_zones = top_data_top_10_zones.domain_zone.value_counts().head(10)
    with open('top_data_top_10_zones.csv', 'w') as f:
        f.write(top_data_top_10_zones.to_csv(index=False, header=False))


def get_stat_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_1_name_len = top_data_df['domain'].str.split('.',expand=True)
    top_data_top_1_name_len.columns =['domain_name','domain_zone','subdomain_1','subdomain_2','subdomain_3']
    top_data_top_1_name_len['name_len'] = top_data_top_1_name_len['domain_name'].str.len()
    top_data_top_1_name_len = top_data_top_1_name_len.sort_values('name_len', ascending=False).head(1)
    with open('top_data_top_1_name_len.csv', 'w') as f:
        f.write(top_data_top_1_name_len.to_csv(index=False, header=False))


def get_stat_lenght_rating_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_name_len_airflow = top_data_df['domain'].str.split('.',expand=True)
    top_data_name_len_airflow.columns =['domain_name','domain_zone','subdomain_1','subdomain_2','subdomain_3']
    top_data_name_len_airflow = top_data_name_len_airflow.assign(lenght=top_data_name_len_airflow["domain_name"].str.len())\
        .sort_values('lenght', ascending=False)\
        .assign(rating=np.arange(1, len(top_data_name_len_airflow)+1))\
        .reset_index(drop=True)\
        .query('domain_name == "airflow"')
    with open('top_data_name_len_airflow.csv', 'w') as f:
        f.write(top_data_name_len_airflow.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_top_10_zones.csv', 'r') as f:
        data_1 = f.read()
    with open('top_data_top_1_name_len.csv', 'r') as f:
        data_2 = f.read()
    with open('top_data_name_len_airflow.csv', 'r') as f:
        data_3 = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(data_1)

    print(f'The longest domain name for date {date}')
    print(data_2)

    print(f'Airflow.es rating by names lenght for date {date}')
    print(data_3)


default_args = {
    'owner': 'n.lopatkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 12, 19),
}
schedule_interval = '50 6 * * *'

dag = DAG('top_10_nlopatkov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_top_10',
                    python_callable=get_stat_top_10,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_stat_longest_name',
                        python_callable=get_stat_longest_name,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_stat_lenght_rating_airflow',
                    python_callable=get_stat_lenght_rating_airflow,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_longest, t2_airflow] >> t3
