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

        
def top_domain_zones():
    top_zones = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_zones = top_zones.domain.str.split('.').str[-1].value_counts().reset_index().head(10).rename(columns={'index' : 'zone', 'domain' : 'cnt'})
    with open('top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))
    

def max_name_len():
    max_name = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_name['len_name'] = max_name.domain.str.split('.').str[0].str.len()
    max_len_name = max_name.sort_values('len_name', ascending=False).reset_index().head(1)
    max_len_name = max_len_name[['domain', 'len_name']]
    with open('max_len_name.csv', 'w') as f:
        f.write(max_len_name.to_csv(index=False, header=False))

def airflow_rank():
    position = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    position = position.domain.str.split('.').str[-2].reset_index()
    position = position.loc[position.domain == 'airflow']
    with open('position.csv', 'w') as f:
        f.write(position.to_csv(index=False, header=False))
    
        
def print_data(ds):
    with open('top_zones.csv', 'r') as f:
        zones_data = f.read()
    with open('max_len_name.csv', 'r') as f:
        max_len_name_data = f.read()
    with open('position.csv', 'r') as f:
        position_airflow_data = f.read()
    date = ds

    print(f'Top domains for date {date}')
    print(zones_data)

    print(f'Max domain lengts {date}')
    print(max_len_name_data)
    
    print(f'Airflow position {date}')
    print(position_airflow_data.split(',')[0])


default_args = {
    'owner': 'm.sychev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 17),
}
schedule_interval = '30 10 * * *'

dag = DAG('lesson_2_sychev', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_domain_zones',
                    python_callable=top_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='max_name_len',
                        python_callable=max_name_len,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
