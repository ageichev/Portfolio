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


def get_dom_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    domain_zone = top_data_df.domain.apply(lambda x: '.'.join(x.split('.')[1:])).value_counts().head(10)
    with open('domain_zone.csv', 'w') as f:
        f.write(domain_zone.to_csv(index=False, header=False))


def get_max_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    domain_max_length_name = top_data_df.domain.apply(lambda x: len(x.split('.')[0])).max()
    domain_max = top_data_df['domain'][top_data_df.domain.apply(lambda x: len(x.split('.')[0])==domain_max_length_name)].sort_values().iloc[0]
    with open('domain_length_name.txt', 'w') as f:
        f.write(domain_max)

        
def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    rank_airflow = str(top_data_df['rank'][top_data_df.domain.str.contains('airflow')].iloc[0])
    with open('rank_airflow.txt', 'w') as f:
        f.write(rank_airflow)


def print_data(ds):
    with open('domain_zone.csv', 'r') as f:
        data_domain_zone = f.read()
    with open('domain_length_name.txt', 'r') as f:
        data_domain_max_length = f.read()
    with open('rank_airflow.txt', 'r') as f:
        data_rank_airflow = f.read()
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(data_domain_zone)

    print(f'Max name domain for date {date}')
    print(data_domain_max_length)
    
    print(f'Rank airflow for date {date}')
    print(data_rank_airflow)


default_args = {
    'owner': 'i-gurkina-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 14),
}
schedule_interval = '0 12 * * *'

dag = DAG('i-gurkina-25_hw_lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_dz = PythonOperator(task_id='get_dom_zone',
                    python_callable=get_dom_zone,
                    dag=dag)

t2_mn = PythonOperator(task_id='get_max_name',
                        python_callable=get_max_name,
                        dag=dag)

t2_af = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_dz, t2_mn, t2_af] >> t3