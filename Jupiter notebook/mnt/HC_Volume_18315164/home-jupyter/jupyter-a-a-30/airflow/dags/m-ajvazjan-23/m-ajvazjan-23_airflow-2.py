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


def get_top10_domains():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.apply(lambda row: row.domain.split('.')[-1], axis=1) 
    top_zones = df.groupby('zone', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10)   
    with open('top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))


def get_longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['name_length'] = df.domain.str.len()    
    longest_name = df.sort_values(['name_length','domain'], ascending=[False,True]).iloc[0].domain        
    with open('longest_name.txt', 'w') as f:
        f.write(longest_name)
        
def aiflow_position():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if df[df.domain == 'airflow.com'].shape[0]:
        output = df[df.domain == 'airflow.com'].iloc[0]['rank']
    else:
        output = 'Domain not found'
    with open('aiflow_position.txt', 'w') as f:
        f.write(str(output))


def print_data(ds):
    with open('top_zones.csv', 'r') as f:
        top_zones = f.read()
    with open('longest_name.txt', 'r') as f:
        longest_name = f.read()
    with open('aiflow_position.txt', 'r') as f:
        aiflow_position = f.read()
        
    date = ds

    print(f'Top-10 zones by domain count for {date}')
    print(top_zones)

    print(f'Domain with the longest name for {date}')
    print(longest_name)
    
    print(f'Airflow.com position for {date}')
    print(aiflow_position)


default_args = {
    'owner': 'm-ajvazjan-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 7),
}
schedule_interval = '0 15 * * *'

dag = DAG('m-ajvazjan-23_airflow', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_domains',
                    python_callable=get_top10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='aiflow_position',
                    python_callable=aiflow_position,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
