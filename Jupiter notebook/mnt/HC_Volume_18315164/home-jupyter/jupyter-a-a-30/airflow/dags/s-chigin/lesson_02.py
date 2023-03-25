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


def get_task_1():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    df['zone'] = df['domain'].str.split('.').str[-1]

    df_agg = df \
                .groupby('zone', as_index = False) \
                .agg(cnt_domains = ('domain', 'count')) \
                .sort_values(by = 'cnt_domains', ascending = False) \
                .head(10)
    
    with open('get_task1_data.csv', 'w') as f:
        f.write(df_agg.to_csv(index=False, header=False))


def get_task_2():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    df['domain_name_length'] = df['domain'].str.len()
    
    longest_domain = df \
                        .sort_values(by = 'domain_name_length', ascending = False) \
                        .reset_index(drop = True)['domain'][0]
    
    with open('get_task2_data.txt', 'w') as f:
        f.write(longest_domain)

        
def get_task_3():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    df['domain_name_length'] = df['domain'].str.len()
    
    airflow_pos = df \
                        .sort_values(by = 'domain_name_length', ascending = False) \
                        .reset_index(drop = True) \
                        .query("domain == 'airflow.com'") \
                        .index[0] + 1
    
    with open('get_task3_data.txt', 'w') as f:
        f.write(str(airflow_pos))


def print_data(ds):
    with open('get_task1_data.csv', 'r') as f:
        task_1_data = f.read()
        
    with open('get_task2_data.txt', 'r') as f:
        task_2_data = f.read()
        
    with open('get_task3_data.txt', 'r') as f:
        task_3_data = f.read()
    date = ds

    print(f'Top 10 domains zones by number of domains for date {date} (output format: zone, cnt_domains):')
    print(task_1_data)

    print(f'\nDomain with longest name for date {date}:')
    print(task_2_data)
    
    print(f"\nPosition of airflow.com according to its name length for date {date}:")
    print(task_3_data)


default_args = {
    'owner': 's-chigin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 25),
}
schedule_interval = '30 16 * * *'

dag = DAG('s-chigin_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t0 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t1 = PythonOperator(task_id='task_1',
                    python_callable=get_task_1,
                    dag=dag)

t2 = PythonOperator(task_id='task_2',
                    python_callable=get_task_2,
                    dag=dag)

t3 = PythonOperator(task_id='task_3',
                    python_callable=get_task_3,
                    dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t0 >> [t1, t2, t3] >> t4

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
