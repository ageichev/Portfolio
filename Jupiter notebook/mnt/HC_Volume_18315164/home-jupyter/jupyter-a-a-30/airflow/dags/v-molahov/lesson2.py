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


def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.str.split('.').str[-1]
    top_10_domain_zone = top_data_df.groupby('domain_zone', as_index=False).domain.count().sort_values('domain', ascending=False).head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))


def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.str.len()
    longest_domain = top_data_df.sort_values(['length', 'domain'], ascending=[False, True]).head(1).domain
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))
        
        
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_zones_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain_data = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()
        
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_zones_data)

    print(f'Longest domain for date {date}')
    print(longest_domain_data)
    
    print(f'Rank airflow.com for date {date}')
    print(airflow_rank_data)


default_args = {
    'owner': 'v-molahov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 22),
}
schedule_interval = '30 9 * * *'

dag = DAG('homework_2_airflow_v-molahov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t3)
#t1.set_downstream(t4)
#t2.set_downstream(t5)
#t3.set_downstream(t5)
#t4.set_downstream(t5)