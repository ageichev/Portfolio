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


# Найти топ-10 доменных зон по численности доменов
def get_top_10_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_10_domain_zones = top_data_df.groupby('domain_zone', as_index=False) \
                            .agg({'domain': 'count'}) \
                            .rename(columns={'domain': 'cnt'}) \
                            .sort_values('cnt', ascending=False) \
                            .head(10)

    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain = top_data_df[top_data_df['domain'].str.len() == max(top_data_df['domain'].str.len())].sort_values('domain').head(1)

    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))


# На каком месте находится домен airflow.com
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']['rank']

    if airflow_rank.empty is True:
        airflow_rank = pd.Series('airflow.com is not in the top 1M web sites')

    with open('airflow_domain_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_domain_rank.csv', 'r') as f:
        airflow_domain_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_domain_zones)

    print(f'The longes domain for date {date}')
    print(longest_domain)
    
    print(f'The airflow rank in top 1M domains for date {date}')
    print(airflow_domain_rank)


default_args = {
    'owner': 'i.loladze',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '0 12 * * *'

dag = DAG('i-loladze-lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zones = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag)

t2_longest_domain = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_airflow_rank = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zones, t2_longest_domain, t2_airflow_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)