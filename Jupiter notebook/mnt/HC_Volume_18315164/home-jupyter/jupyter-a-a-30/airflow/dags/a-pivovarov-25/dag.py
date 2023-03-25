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

def get_10_top_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain_zones = top_data_df['domain_zone'].value_counts().reset_index().head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))


def get_longest_length_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length_domain'] = top_data_df['domain'].str.len()
    longest_length_domain = top_data_df.sort_values('domain') \
                        .sort_values('length_domain', ascending=False) \
                        .head(1)['domain'].values[0]
    with open('longest_length_domain.txt', 'w') as f:
        f.write(longest_length_domain)

def get_airflow_rank():
    try:
        top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
        airflow_rank = top_data_df.loc(top_data_df.domain == 'airflow.com')['rank'].values[0]
    except:
        airflow_rank = 'No domain'
    
    with open('airflow_com.txt', 'w') as f:
        f.write(airflow_rank)

def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
        
    with open('longest_length_domain.txt', 'r') as f:
        longest_length_domain = f.read()
        
    with open('airflow_com.txt', 'r') as f:
        airflow_com = f.read()
        
    date = ds

    print(f'Top 10 domain zones by number of domains for date {date}')
    print(top_10_domain_zones)

    print(f'The longest domain for date {date}')
    print(longest_length_domain)
    
    print(f'The rank of airflow.com for date {date}')
    print(airflow_com)



default_args = {
    'owner': 'a.pivovarov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2022, 10, 31),
}
schedule_interval = '30 9 * * *'


dag = DAG('a-pivovarov-25', default_args=default_args, schedule_interval=schedule_interval)



t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_10_top_domain_zones',
                    python_callable=get_10_top_domain_zones,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_longest_length_domain',
                        python_callable=get_longest_length_domain,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3