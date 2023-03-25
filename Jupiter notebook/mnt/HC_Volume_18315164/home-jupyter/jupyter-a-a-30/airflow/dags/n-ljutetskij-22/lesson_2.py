import requests
import pandas as pd
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_10_zone():    
    # Найти топ-10 доменных зон по численности доменов
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_doms['domain_zone'] = top_doms.domain.str.extract(r'.+\.(.+)')
    top_10 = (top_doms['domain_zone']
              .value_counts().head(10)
              .to_frame().reset_index()['index']     
             )
    
    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))

def longest_name():
    # Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_doms['domain_name_len'] = top_doms.domain.str.len()    
    max_len = top_doms.domain_name_len.max()
    name = top_doms[top_doms.domain_name_len == max_len].sort_values(by='domain').head(1)['domain']
    
    with open('longest_name.csv', 'w') as f:
        f.write(name.to_csv(index=False, header=False))      

        
def airflow_rank():
    # На каком месте находится домен airflow.com?
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    try:
        air = top_doms.loc[top_doms.domain.str.contains('airflow.com'), 'rank'].values[0]    
    except:
        air = 'airflow.com is absent in data from "http://s3.amazonaws.com/alexa-static/top-1m.csv"'
        
    with open('airflow_rank.txt', 'w') as f:
        f.write(str(air))

def print_data(ds):
    with open('top_10_zone.csv', 'r') as f:
        top_10_zone = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()        
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()
        
    date = ds         

    print(f'Top-10 domain zones for date {date}:')
    print(top_10_zone)

    print(f'Longest domain name for date {date}:')
    print(longest_name)
    
    print(f'airflow.com rank for date {date}:')
    print(airflow_rank)


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'n-ljutetskij-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 19),
}

schedule_interval = '0 18 * * *'
dag = DAG('lesson_2_task_4_n-ljutetskij-22', default_args=default_args, schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_zone',
                    python_callable=top_10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='longest_name',
                    python_callable=longest_name,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
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