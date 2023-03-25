import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    def get_zone(st):
        return st.split('.')[-1]
    top_data_df['zone'] = top_data_df['domain'].apply(get_zone)
    top_10_domains = top_data_df.groupby('zone').agg({'domain':'count'}).sort_values('domain',ascending=False).head(10)
    with open('top_10_domains.csv', 'w') as f:
            f.write(top_10_domains.to_csv(index=True, header=False))
            
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.str.len()
    longest_domain = top_data_df.sort_values(['length','domain'],ascending=[False,True]).domain.head(1)
    with open('longest_domain.csv', 'w') as f:
           f.write(longest_domain.to_csv(index=True, header=False))
            
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']
    msg = ''
    if airflow_rank.shape[0] == 0:
        msg = 'No rank information for "airflow.com".'
    else:
        rank = airflow_rank[0].iloc[0,0]
        msg = f'Domain "airflow.com" is rank {rank}.'
    with open('airflow_rank.csv', 'w') as f:
           f.write(msg)
    


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domains.csv', 'r') as f:
        top_10_domains = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()        
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_domains)

    print(f'Longest domain for date {date}')
    print(longest_domain)
    
    print(f'airflow.com domain rank for date {date}')
    print(airflow_rank)


    
default_args = {
    'owner': 'ar.medvedev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 25),
    'schedule_interval': '20 15 * * *'
}
dag = DAG('dag-lesson-2-ar-medvedev', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
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
