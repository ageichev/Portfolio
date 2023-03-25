import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# Найти топ-10 доменных зон по численности доменов
def domain_zones():
    top_dom = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_dom['domain_zone'] = top_dom['domain'].apply(lambda x: x.split('.')[-1]) 
    top_10_dom = top_dom['domain_zone'].value_counts().head(10).to_frame().reset_index().rename(columns= {'index': 'domain_zone', 'domain_zone': 'num'})
    with open('top_10_dom.csv', 'w') as f:
        f.write(top_10_dom.to_csv(index=False, header=False))
                    
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def domain_len():
    top_dom = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_dom['domain_len'] = top_dom['domain'].apply(lambda x: len(x)) 
    longest_name = top_dom.sort_values(['domain_len', 'domain'], ascending=[False, True]).iloc[0].domain
    with open('longest_name.txt', 'w') as f:
        f.write(longest_name)
                
# На каком месте находится домен airflow.com?
def airflow():
    top_dom = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    if 'airflow.com' in top_dom['domain']:
        rank = int(top_dom[top_dom['domain'] == 'airflow.com']['rank'])
    else:
        rank = 'airflow.com not in the data'
    with open('rank.txt', 'w') as f:
        f.write(rank)

def print_data(ds):
    with open('top_10_dom.csv', 'r') as f:
        zones_by_num = f.read()
    with open('longest_name.txt', 'r') as f:
        max_domain_name = f.read()
    with open('rank.txt', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(zones_by_num)

    print(f'Max domain lenght for date {date}')
    print(max_domain_name)
    
    print(f'Airflow.com rank for date {date}')
    print(airflow_rank)
                
default_args = {
    'owner': 'i-evsa-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 8, 2),
}
schedule_interval = '05 17 * * *'

dag = DAG('lesson2-i-evsa', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id ='top_domain_zones',
                    python_callable = domain_zones,
                    dag=dag)

t2 = PythonOperator(task_id='domain_zones_lenght',
                    python_callable = domain_len,
                    dag=dag)

t3 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow,
                        dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3] >> t4                

