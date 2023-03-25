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


def get_top_zone():
    top_zone_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_zone_df.domain = top_zone_df.domain.apply(lambda x: x.split('.'))
    top_zone_df.domain = top_zone_df.domain.apply(lambda x: x[-1])
    top_zone_df = top_zone_df.groupby('domain', as_index=False) \
        .agg({'rank':'count'}) \
        .sort_values('rank', ascending=False) \
        .rename(columns={'domain':'zone', 'rank':'count'}) \
        .head(10)
    
    with open('top_zone_top_10.csv', 'w') as f:
        f.write(top_zone_df.to_csv(index=False, header=False))


def get_longest_domain():
    longest_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain_df['length'] = longest_domain_df.domain.apply(lambda x: len(x))
    longest_domain = longest_domain_df.query('length == length.max()').sort_values('domain').head(1)
    longest_domain = longest_domain.reset_index().at[0, 'domain']
    
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)


def get_airflow_rank():
    airflow_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        pos = airflow_df.query('domain == "airflow.com"').at[0, 'rank']
        res_string = "Airflow.com rank is {}".format(pos)
    except KeyError:
        res_string = "Airflow.com not found"
    
    with open('airflow_rank.txt', 'w') as f:
        f.write(res_string)


def print_data(ds):
    with open('top_zone_top_10.csv', 'r') as f:
        zone_data = f.read()
    with open('longest_domain.txt', 'r') as f:
        long_domain = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(zone_data)

    print(f'The longest domain name for date {date}')
    print(long_domain)

    print(airflow_rank)
    print(f'For date {date}')


default_args = {
    'owner': 'e.fedorchuk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 25),
}
schedule_interval = '0 15 * * *'

dag = DAG('domain_stat_e.fedorchuk', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_top_zone',
                    python_callable=get_top_zone,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_longest, t2_rank] >> t3
