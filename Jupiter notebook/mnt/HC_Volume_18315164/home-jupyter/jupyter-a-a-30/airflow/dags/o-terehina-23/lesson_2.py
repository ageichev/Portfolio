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
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
    
    
def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.str.split('.').str[-1]
    top_data_top_10_domain_zone = top_data_df.groupby('domain_zone',as_index=False)\
                                             .agg({'domain':'nunique'})\
                                             .sort_values('domain',ascending=False)\
                                             .head(10)
    with open('top_data_top_10_domain_zone.csv', 'w') as f:
        f.write(top_data_top_10_domain_zone.to_csv(index=False, header=False))
    
    
def get_domain_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df.domain.str.len()
    top_data_df_domain_len = top_data_df.sort_values('len_domain',ascending=False)
    with open('top_data_df_domain_len.csv', 'w') as f:
        f.write(top_data_df_domain_len.to_csv(index=False, header=False))
    
    
def get_domain_airflow_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df.domain.str.len()
    domain_airflow_com = top_data_df.query("domain == 'airflow.com'")
    with open('domain_airflow_com.csv', 'w') as f:
    f.write(domain_airflow_com.to_csv(index=False, header=False))
    
def print_data(ds):
    with open('top_data_top_10_domain_zone.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_df_domain_len.csv', 'r') as f:
        all_data_domain_zone = f.read()
    with open('domain_airflow_com.csv', 'r') as f:
        all_data_airflow_com = f.read()
    date = ds  
        
    print(f'Top domain zone for date {date}')
    print(all_data)
    
    print(f'Domain len for date {date}')
    print(all_data_domain_zone)
    
    print(f'Domain Airflow.com for date {date}')
    print(all_data_airflow_com)
    
default_args = {
  'owner': 'o-terehina-23',
  'depends_on_past': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=5),
  'start_date': datetime(2022, 8, 23),
    }
schedule_interval = '30 16 * * *'
    
dag = DAG('o-terehina-23', default_args=default_args, schedule_interval=schedule_interval)
    
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
    
t2 = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)
    
t3 = PythonOperator(task_id='get_domain_len',
                    python_callable=get_domain_len,
                    dag=dag)
    
t4 = PythonOperator(task_id='get_domain_airflow_com',
                    python_callable=get_domain_airflow_com,
                    dag=dag)
    
t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)
    
t1 >> t2 >> t3 >> t4 >> t5
    
   
   
 