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


def get_stat_domain():
    top_domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domains_df['region'] = top_domains_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domains = top_domains_df.groupby('region', as_index=False) \
        .agg({'rank': 'count'}) \
        .rename(columns={'rank': 'amount'}) \
        .sort_values('amount', ascending=False) \
        .reset_index(drop=True) \
        .head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))


def get_longest_domain():
    top_domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    s = top_domains_df.domain.str.len().sort_values(ascending=False).index
    df = top_domains_df.reindex(s)
    df = df.reset_index(drop=True)
    with open('longest_domain.txt', 'w') as f:
        f.write(df.iloc[0].domain)


def get_airflow_placement():
    top_domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_placement = top_domains_df.query("domain == 'airflow.com'")
    if len(airflow_placement) == 0:
        with open('airflow_placement.txt', 'w') as f:
            f.write('Not listed')
    else:
        with open('airflow_placement.txt', 'w') as f:
            f.write(airflow_placement['rank'].iloc[0])


def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        all_data_domains = f.read()
    with open('longest_domain.txt', 'r') as f:
        all_data_longest = f.read()
    with open('airflow_placement.txt', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top 10 domains area for date {date}:')
    print(all_data_domains)

    print(f'The longest domain name for date {date}:')
    print(all_data_longest)
    print(f'With length:')
    print(len(all_data_longest))

    print(f'Airflow.com placement for date {date}:')
    print(all_data_airflow)

default_args = {
    'owner': 'aivannikova1',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 18),
}
schedule_interval = '0 15 * * *'

aivannikova1_dag = DAG('v-ivannikov_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=aivannikova1_dag)

t2 = PythonOperator(task_id='get_stat_domain',
                    python_callable=get_stat_domain,
                    dag=aivannikova1_dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=aivannikova1_dag)

t4 = PythonOperator(task_id='get_airflow_placement',
                        python_callable=get_airflow_placement,
                        dag=aivannikova1_dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=aivannikova1_dag)

t1 >> [t2, t3, t4] >> t5

