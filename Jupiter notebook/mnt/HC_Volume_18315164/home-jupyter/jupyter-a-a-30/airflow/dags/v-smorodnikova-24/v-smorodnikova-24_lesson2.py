from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишет такси в питоне

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zones'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_zones = pd.DataFrame(top_data_df.domain_zones.value_counts()).head(10) \
                    .reset_index().rename(columns = {'domain_zones': 'domains_quantity', 'index': 'domain_zone'})
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))
            
def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].apply(lambda x: len(x))
    max_length  = top_data_df['domain_length'].max()
    longest_domain = f"Самое длинной имя ({top_data_df[top_data_df.domain_length == max_length]['domain_length'].values[0]} символов) \
у домена {top_data_df[top_data_df.domain_length == max_length]['domain'].values[0]}"
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)
    
def airlow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if len(top_data_df[top_data_df.domain == 'airflow.com']['rank']) > 0:
        aiflow_rank = f'Домен airflow.com на {top_data_df[top_data_df.domain == "airflow.com"]["rank"][0]} месте'
    else:
        aiflow_rank = 'Домена airflow.com нет в данных'
    with open('aiflow_rank.txt', 'w' ) as f:
            f.write(aiflow_rank)
        
def print_results(ds):
    with open('top_10_zones.csv', 'r') as f:
        answer_1 = f.read()
    with open('longest_domain.txt', 'r') as f:
        answer_2 = f.read()
    with open('aiflow_rank.txt', 'r') as f:
        answer_3 = f.read()
    
    date = ds
    
    print(f" 1. Топ-10 доменных зон по численности доменов на {date}")
    print(answer_1)
    
    print(" 2. " + answer_2 + f" на {date}")
    
    print(" 3. " + answer_3 + f" на {date}")
        
default_args = {
    'owner': 'v.smorodnikova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 28),
    'schedule_interval': '30 10 * * *'
}
dag = DAG('v-smorodnikova-24_lesson_2_dag', default_args=default_args)    
    

t1 = PythonOperator(task_id = 'get_data',
                    python_callable = get_data,
                    dag = dag)

t2 = PythonOperator(task_id = 'top_10_domains',
                    python_callable = top_10_domains,
                    dag = dag)

t3 = PythonOperator(task_id = 'longest_domain',
                    python_callable = longest_domain,
                    dag = dag)
    
t4 = PythonOperator(task_id = 'airlow_rank',
                    python_callable = airlow_rank,
                    dag = dag)    
    
t5 = PythonOperator(task_id = 'print_results',
                    python_callable = print_results,
                    dag = dag)    
    
t1 >> [t2, t3, t4] >>t5   
    