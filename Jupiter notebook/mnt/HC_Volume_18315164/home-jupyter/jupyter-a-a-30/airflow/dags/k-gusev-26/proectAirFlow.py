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


def get_domain_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['end2'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_data_domen_10 = top_data_df.groupby('end2')['rank'].count().sort_values(ascending=False).reset_index()
    top_data_domen_10 = top_data_domen_10.head(10)
    with open('top_data_domain_10.csv', 'w') as f:
        f.write(top_data_domen_10.to_csv(index=False, header=False))


def get_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df['domain'].apply(lambda x: len(x))
    top_data_len_domain = top_data_df[['domain', 'len_domain']].sort_values(by='len_domain', ascending=False) \
        .head(1)['domain']
    with open('top_data_len_domain.csv', 'w') as f:
        f.write(top_data_len_domain.to_csv(index=False, header=False))

def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[['rank', 'domain']][top_data_df['domain'] == 'airflow.com']['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_domain_10.csv', 'r') as f:
        domen_10 = f.read()
    with open('top_data_len_domain.csv', 'r') as f:
        len_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'top 10 domain zones by the number of domains {date}')
    print(domen_10)

    print(f'the domain with the longest name {date}')
    print(len_domain)

    print(f'airflow.com located on place {date}')
    print(airflow_rank)


default_args = {
    'owner': 'k.gusev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '50 6 * * *'

dag = DAG('k_gusev_26', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_10',
                    python_callable=get_domain_10,
                    dag=dag)

t2_len = PythonOperator(task_id='get_len_domain',
                        python_callable=get_len_domain,
                        dag=dag)
t2_airflow = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_len, t2_airflow] >> t3
