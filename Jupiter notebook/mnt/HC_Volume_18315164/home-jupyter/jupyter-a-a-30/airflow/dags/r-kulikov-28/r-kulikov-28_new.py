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



def get_data_r_kulikov_28_new():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_stat_r_kulikov_28_new():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain = top_data_df['domain'].head(10)
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

    long_domain = top_data_df.loc[top_data_df.domain.astype(str).map(len).argmax(), 'domain']
    longest_domain = top_data_df[top_data_df['domain'] == long_domain]
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

    airflow_domain = top_data_df[top_data_df['domain'] == 'airflow.com']
    with open('airflow_domain.csv', 'w') as f:
        f.write(airflow_domain.to_csv(index=False, header=False))

def print_data_r_kulikov_28_new(ds): # передаем глобальную переменную airflow
    with open('top_10_domain.csv', 'r') as f:
        all_data_top_10_domain = f.read()
    with open('longest_domain.csv', 'r') as f:
        all_data_longest_domain = f.read()
    with open('airflow_domain.csv', 'r') as f:
        all_data_airflow_domain = f.read()
    
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data_top_10_domain)

    print(f'The longest domain for date {date}')
    print(all_data_longest_domain)
    
    print(f'The rank for airflow domain for date {date}')
    print(all_data_airflow_domain)


    
default_args = {
    'owner': 'r-kulikov-28',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 12),
    'schedule_interval': '0 15 * * *'
}
dag_r_kulikov_28_new = DAG('r-kulikov-28_new', default_args=default_args)



t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data_r_kulikov_28_new,
                    dag=dag_r_kulikov_28_new)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat_r_kulikov_28_new,
                    dag=dag_r_kulikov_28_new)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data_r_kulikov_28_new,
                    dag=dag_r_kulikov_28_new)


t1 >> t2 >> t3