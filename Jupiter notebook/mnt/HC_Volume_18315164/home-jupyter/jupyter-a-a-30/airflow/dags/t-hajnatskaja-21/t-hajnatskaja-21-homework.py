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


def get_top_10_dom_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['dom_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    data_top_10_dom_zone = top_data_df['dom_zone'].value_counts().reset_index().head(10)
    with open('top_10_dom_zone.csv', 'w') as f:
        f.write(top_10_dom_zone.to_csv(index=False, header=False))


def get_longest_names():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['longest_names'] = top_data_df.domain.apply(lambda x: len(x))
    long_dom_name = top_data_df.sort_values('length_name', ascending = False).head(1)
    with open('longest_dom_names.csv', 'w') as f:
        f.write(longest_dom_names.to_csv(index=False, header=False))



def get_airflow_com_rank ():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_com_rank = top_data_df.query("domain.str.contains('airflow.com')")['rank']
    with open('airflow_com_rank.csv', 'w') as f:
        f.write(airflow_com_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_dom_zone.csv', 'r') as f:
        top_ten_dom = f.read()
    with open('longest_dom_names.csv', 'r') as f:
        longest_domain_name = f.read()
    with open('airflow_com_rank.csv', 'r') as f:
        airflowcom = f.read()   
    date = ds

    print(f'Top 10 domains zone for date {date}')
    print(all_data)

    print(f'Longest domain name for date {date}')
    print(all_data_com)
    
    print(f'The rank of airflow.com for date {date}')
    print(all_data_com)

default_args = {
    'owner': 't-hajnatskaja-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 3),
}
schedule_interval = '0 15 * * *'

dag_khaynatskaya = DAG('amazon_t-hajnatskaja-21', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_khaynatskaya)

t2 = PythonOperator(task_id='get_top_10_dom_zone',
                    python_callable= get_top_10_dom_zone,
                    dag=dag_khaynatskaya)

t3 = PythonOperator(task_id='get_longest_names',
                        python_callable=get_longest_names,
                        dag=dag_khaynatskaya)

t4 = PythonOperator(task_id='get_airflow_com_rank',
                    python_callable=get_airflow_com_rank,
                    dag=dag_khaynatskaya)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_khaynatskaya)


t1 >> [t2, t3, t4] >> t5




