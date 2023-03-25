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


def get_top_10_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_10_zones = top_data_df \
    .groupby('zone', as_index=False) \
    .agg({'domain': 'count'}) \
    .sort_values('domain', ascending=False) \
    .rename(columns={'domain': 'quantity'}) \
    .reset_index(drop=True) \
    .head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))


def get_top_len_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.domain.str.len()
    top_len_dom = top_data_df \
    .sort_values('domain_length', ascending=False) \
    .reset_index(drop=True) \
    .head(1)
    top_len_dom = top_len_dom[['domain']]
    with open('top_len_dom.csv', 'w') as f:
        f.write(top_len_dom.to_csv(index=False, header=False))
        

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')[['domain', 'rank']]
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('top_len_dom.csv', 'r') as f:
        top_len_dom = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 zones by domain quantity for date {date}')
    print(top_10_zones)

    print(f'The logest domain name for date {date}')
    print(top_len_dom)
    
    print(f'airflow.com rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'a-rykov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 2, 18),
}
schedule_interval = '0 10 * * *'

dag = DAG('a-rykov_domain_zones', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_len_dom',
                        python_callable=get_top_len_dom,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_top_len_dom,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)