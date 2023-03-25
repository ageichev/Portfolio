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


def get_domzones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x : x.rsplit(".")[len(x.rsplit("."))-1])
    top_data_top_10 = top_data_df.domain_zone.value_counts() \
        .reset_index().rename(columns={'domain_zone': 'count','index':'domain_zone'}).head(10)

    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))



def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['dom_len'] = top_data_df['domain'].apply(lambda x : len(x))
    top_data_df = top_data_df[top_data_df['dom_len'] == top_data_df['dom_len'].max()].astype(str)    
    longest_domain = top_data_df.sort_values('domain').reset_index().domain
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)
    
def airflow_domlen_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_domlen_rank = top_data_df[top_data_df['domain'] == 'airflow.su']['rank'].astype(str)
    with open('airflow_domlen_rank.txt', 'w') as f:
        f.write('%d' % airflow_domlen_rank)
        



def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10_data = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
    with open('airflow_domlen_rank.txt', 'r') as f:
        airflow_domlen_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(top_10_data)

    print(f'longest domain name for date {date}')
    print(longest_domain)
    
    print(f'airflow.su domain rank for date {date}')
    print(airflow_domlen_rank)


default_args = {
    'owner': 'a-jakovlev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 29),
}
schedule_interval = '59 20 * * *'

a_jakovlev_dag = DAG('a_jakovlev_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=a_jakovlev_dag)

t2_1 = PythonOperator(task_id='get_domzones',
                    python_callable=get_domzones,
                    dag=a_jakovlev_dag)

t2_2 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=a_jakovlev_dag)

t2_3 = PythonOperator(task_id='airflow_domlen_rank',
                        python_callable=airflow_domlen_rank,
                        dag=a_jakovlev_dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=a_jakovlev_dag)

t1 >> [t2_1, t2_2, t2_3] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
