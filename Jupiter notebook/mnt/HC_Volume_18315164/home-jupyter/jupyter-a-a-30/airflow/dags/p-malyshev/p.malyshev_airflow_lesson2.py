import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


#methods
def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

        
def get_top_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top10_domains = top_data_df.groupby('domain_zone', as_index = False)\
                            .agg({'rank':'count'})\
                            .rename(columns={'rank':'count'})\
                            .sort_values('count', ascending=False)\
                            .head(10)\
                            .reset_index(drop=True)
    with open('top10_domains.csv', 'w') as f:
        f.write(top10_domains.to_csv(index=False, header=False))

    
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name_lenght'] = top_data_df['domain'].str.len()
    longest_domain = top_data_df[top_data_df.name_lenght == top_data_df.name_lenght.max()].domain.item()
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain)

    
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']
    if airflow_rank['rank'].to_list():
        rank_ = print(airflow_rank['rank'].to_list()[0])
    else:
        rank_ = 'airflow.com not in list'
    with open('airflow_rank.csv', 'w') as f:
        f.write(rank_)
    
    
def print_data(ds):
    with open('top10_domains.csv', 'r') as f:
        top10_domains_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain_data = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()
    date = ds

    print(f'Top 10 domains at {date}:')
    print(top10_domains_data)

    print(f'Domain with longest name at {date}:')
    print(longest_domain_data + '\n')
    
    print(f'Airflow.com rank at {date}:')
    print(airflow_rank_data)
    
#airflow
default_args = {
    'owner': 'p.malyshev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 16),
}
schedule_interval = '0 8 * * *'

dag_pavel_malyshev = DAG('airflow_lesson2_pmalyshev', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_pavel_malyshev)

t2_top10 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag_pavel_malyshev)

t2_longest = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag_pavel_malyshev)

t2_airflow = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag_pavel_malyshev)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_pavel_malyshev)

t1 >> [t2_top10, t2_longest, t2_airflow] >> t3
