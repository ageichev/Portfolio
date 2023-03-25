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


def get_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = ['.'.join(map(str, x.split('.')[1: ])) for x in top_data_df.domain]
    top_data_ten_by_domain = top_data_df.value_counts('domain_zone').reset_index().head(10).rename(columns={0: 'num_count'})
    with open('top_data_ten_by_domain.csv', 'w') as f:
        f.write(top_data_ten_by_domain.to_csv(index=False, header=False))


def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.domain.str.len()
    top_data_df['rank'] = top_data_df['domain_length'].rank(method='min', ascending=False)
    top_data_df = top_data_df.query("rank == 1.0").sort_values('domain').head(1)['domain']
    with open('longest_domain.csv', 'w') as f:
        f.write(top_data_df.to_csv(index=False, header=False))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query("domain == 'airflow.com'")['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data():
    with open('top_data_ten_by_domain.csv', 'r') as f:
        all_data_ten = f.read()
    with open('longest_domain.csv', 'r') as f:
        all_data_top = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_rank = f.read()

    print(f'The top ten domains by domain frequency:')
    print(all_data_ten)

    print(f'The longest domain name is:')
    print(all_data_top)

    print(f'Airflow domain takes the following rank:')
    print(all_data_rank)


default_args = {
    'owner': 'a-bubenschikov-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 1),
}
schedule_interval = '0 0 * * *'

abuben = DAG('abubenchshikov_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=abuben)

t2_top_ten = PythonOperator(task_id='get_domain_zones',
                    python_callable=get_domain_zones,
                    dag=abuben)

t2_longest_domain = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=abuben)

t2_airflow_rank = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=abuben)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=abuben)

t1 >> [t2_top_ten, t2_longest_domain, t2_airflow_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)