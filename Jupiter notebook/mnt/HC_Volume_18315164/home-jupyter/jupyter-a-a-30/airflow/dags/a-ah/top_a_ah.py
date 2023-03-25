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
    top_data_ah = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data_ah)


def get_top_ah():
    top_data_ah = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_ah['domain'] = top_data_ah['domain'].str.partition('.')[2]
    top_data_domain_count = top_data_ah.groupby('domain',as_index = False).agg({'rank':'count'})\
    .sort_values(by='rank', ascending=False).head(10)
    with open('top_data_domain_count.csv', 'w') as f:
        f.write(top_data_domain_count.to_csv(index=False, header=False))

def get_top_ah_task2():
    top_data_ah = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_len_domain = sorted(top_data_ah.domain,key=len, reverse=True)[0]
    with open('top_len_domain.txt', 'w') as f:
        f.write(top_len_domain)
        
def get_top_ah_task3():
    top_data_ah = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_ah[top_data_ah.domain == 'airflow.com'].shape[0]:
        airflow_rank = top_data_ah[top_data_ah.domain == 'airflow.com'].iloc[0]['rank']
    else:
        airflow_rank = 'No  domain aifflow.com in file'
    with open('airflow_rank.txt', 'w') as f:
        f.write(str(airflow_rank))



def print_data_ah(ds):
    with open('top_data_domain_count.csv', 'r') as f:
        all_data = f.read()
    with open('top_len_domain.txt', 'r') as f:
        all_data_len = f.read()
    with open('airflow_rank.txt', 'r') as f:
        all_data_rank = f.read()    
    date = ds

    print(f'Top 10 domains by count for date {date}')
    print(all_data)

    print(f'Top len domain for date {date}')
    print(all_data_len)
    
    print(f'Airflow.com rank is for date {date}')
    print(all_data_rank)

default_args = {
    'owner': 'a-ah',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 1),
}
schedule_interval = '5 5 * * *'

dag = DAG('top_a_ah', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_ah',
                    python_callable=get_top_ah,
                    dag=dag)

t2_task2 = PythonOperator(task_id='get_top_ah_task2',
                        python_callable=get_top_ah_task2,
                        dag=dag)
                        
t2_task3 = PythonOperator(task_id='get_top_ah_task3',
                        python_callable=get_top_ah_task3,
                        dag=dag)

t3 = PythonOperator(task_id='print_data_ah',
                    python_callable=print_data_ah,
                    dag=dag)

t1 >> t2 >> t2_task2 >> t2_task3 >> t3
