import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data_r_dubchak():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_domainzon_r_dubchak():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_domain_10 = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_data_domain_10 = top_data_domain_10.value_counts().reset_index().head(10)
    with open('top_data_domain_10.csv', 'w') as f:
        f.write(top_data_domain_10.to_csv(index=False, header=False))


def get_lname_r_dubchak():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len_domain = top_data_df[top_data_df.domain.str.len()==top_data_df.domain.str.len().max()]
    max_len_domain = max_len_domain.sort_values(by='domain').head(1)
    with open('max_len_domain.csv', 'w') as f:
        f.write(max_len_domain.to_csv(index=False, header=False))

def get_pozition_airflow_r_dubchak():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    lok_for_air = top_data_df[top_data_df.domain=='airflow.com']
    with open('lok_for_air.csv', 'w') as f:
        f.write(lok_for_air.to_csv(index=False, header=False))
        
def output_info_r_dubchak(ds):
    with open('top_data_domain_10.csv', 'r') as f:
        all_data = f.read()
    with open('max_len_domain.csv', 'r') as f:
        all_data_max_len = f.read()
    with open('lok_for_air.csv', 'r') as f:
        all_data_lok_for_air = f.read()    
        date = ds

    print(f'Top count domain for date {date}')
    print(all_data)

    print(f'Name max len in domain for date {date}')
    print(all_data_max_len)
    
    print(f'Rank Airflow for date {date}')
    print(all_data_lok_for_air)

default_args = {
    'owner': 'r-dubchak-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 18),
}
schedule_interval = '0 14 * * *'

dag = DAG('r-dubchak-22', default_args=default_args, schedule_interval=schedule_interval)

t1_get_data = PythonOperator(task_id='get_data_r_dubchak',
                    python_callable=get_data_r_dubchak,
                    dag=dag)

t2_get_top_domainzon = PythonOperator(task_id='get_top_domainzon_r_dubchak',
                    python_callable=get_top_domainzon_r_dubchak,
                    dag=dag)

t2_get_lname = PythonOperator(task_id='get_lname_r_dubchak',
                        python_callable=get_lname_r_dubchak,
                        dag=dag)

t2_get_pozition = PythonOperator(task_id='get_pozition_airflow_r_dubchak',
                        python_callable=get_pozition_airflow_r_dubchak,
                        dag=dag)

t3_output_info = PythonOperator(task_id='output_info_r_dubchak',
                    python_callable=output_info_r_dubchak,
                    dag=dag)

t1_get_data >> [t2_get_top_domainzon,t2_get_lname,t2_get_pozition] >>t3_output_info

