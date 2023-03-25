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


def top_10_doms():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_doms= top_data_df['domain'].str.rsplit(".", n=1, expand=True)[1].value_counts().head(10)
    with open('top_10_doms.csv', 'w') as f:
        f.write(top_10_doms.to_csv(index=False, header=False))


def dom_max_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    dom_max_len = top_data_df[top_data_df['domain'].str.len() == top_data_df['domain'].str.len().max()]['domain']
    with open('dom_max_len.csv', 'w') as f:
        f.write(dom_max_len.to_csv(index=False, header=False))
        
        
def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_doms.csv', 'r') as f:
        all_data_1 = f.read()
    with open('dom_max_len.csv', 'r') as f:
        all_data_2 = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_3 = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data_1)

    print(f'Domain with max length for date {date}')
    print(all_data_2)
    
    print(f'Airflow rank for date {date}')
    print(all_data_3)



default_args = {
    'owner': 'v-skripskaja-29',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 7),
    'schedule_interval': '0 10 * * *'
}

dag = DAG('v-skripskaja-29', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='top_10_doms',
                    python_callable=top_10_doms,
                    dag=dag)

t2_max_len = PythonOperator(task_id='dom_max_len',
                        python_callable=dom_max_len,
                        dag=dag)

t2_airflow_rank = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2_top, t2_max_len, t2_airflow_rank] >> t3
