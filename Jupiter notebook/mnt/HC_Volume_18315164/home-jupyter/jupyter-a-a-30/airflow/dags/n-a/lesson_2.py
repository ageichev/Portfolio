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

def get_zone_top():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_zone =  top_data_df.domain.apply(lambda x: x.split('.')[-1])
    domain_zone_top = domain_zone.value_counts().head(10).reset_index()
    with open('domain_zone_top.csv', 'w') as f:
        f.write(domain_zone_top.to_csv(index=False, header=False))
        
def get_max_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: x.split('.')[0]).str.len()
    max_domain_name = top_data_df.sort_values(by=['domain_len', 'domain'], ascending=[False, True]).head(1)['domain']
    with open('max_domain_name.csv', 'w') as f:
        f.write(max_domain_name.to_csv(index=False, header=False))
        
def get_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_rank = top_data_df.query("domain == 'airflow.com'")['rank']
    if domain_rank.empty:
        domain_rank = 'domain not found'
    else:
        domain_rank
    with open('domain_rank.txt', 'w') as f:
        f.write(str(domain_rank))


def print_data(ds):
    with open('domain_zone_top.csv', 'r') as f:
        all_data_top = f.read()
    with open('max_domain_name.csv', 'r') as f:
        all_data_name = f.read()
    with open('domain_rank.txt', 'r') as f:
        all_data_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(all_data_top)

    print(f'The longest domain name for date {date}')
    print(all_data_name)

    print(f'Rank of the domain airflow.com for date {date}')
    print(all_data_rank)


default_args = {
    'owner': 'n-a',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 20),
}
schedule_interval = '0 10 * * *'

dag = DAG('nanokhina_dag_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_zone_top',
                    python_callable=get_zone_top,
                    dag=dag)

t2_name = PythonOperator(task_id='get_max_name',
                        python_callable=get_max_name,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_rank',
                        python_callable=get_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_name, t2_rank] >> t3


