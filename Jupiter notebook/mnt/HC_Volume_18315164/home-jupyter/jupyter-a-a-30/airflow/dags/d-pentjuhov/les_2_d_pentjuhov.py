import requests
import re
import pandas as pd
from datetime import timedelta
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO

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


def get_top_domain_zones():
    domain_zones = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_zones['zone'] = domain_zones.domain.str.split('.').str[-1]
    domain_zones_top_10 = domain_zones.zone.value_counts().head(10).reset_index().rename(columns={'index':'zone', 'zone':'count'})
    with open('domain_zones_top_10.csv', 'w') as f:
        f.write(domain_zones_top_10.to_csv(index=False, header=False))
        
        
def get_max_len_name_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain']= top_data_df.domain.str.len()
    data_max_domain = top_data_df.sort_values('len_domain', ascending= False).head(1)
    with open('data_max_domain.csv', 'w') as f:
        f.write(data_max_domain.to_csv(index=False, header=False))

        
        
def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df.domain.str.len()
    top_data_df = top_data_df.sort_values('len_domain', ascending= False).reset_index()
    airflow_com = top_data_df.query("domain == 'airflow.com'").reset_index()
    airflow_com = airflow_com[['domain', 'level_0']].rename(columns = {'level_0':'place'})
    if airflow_com.empty:
        res = 'Not in the list'
    else:
        res = airflow_com.place
    print(res)
    with open('airflow_com.txt', 'w') as f:
        f.write(str(res))    
        
        

def print_data(ds):
    with open('domain_zones_top_10.csv', 'r') as f:
        zones_data = f.read()
    with open('data_max_domain.csv', 'r') as f:
        long_data = f.read()
    with open('airflow_com.txt', 'r') as f:
        airflow_data = f.read()
    date = ds

    print(f'Top domains by zone for date {date}')
    print(zones_data)

    print(f'Longest domain name for date {date}')
    print(long_data)
    
    print(f'Rank for airflow.com for date {date}')
    print(airflow_data)


default_args = {
    'owner': 'd.pentjuhov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 26),
}


schedule_interval = '25 10 * * *'


dag = DAG('top_10_d_pentjuhov', default_args=default_args, schedule_interval=schedule_interval)



t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain_zones',
                    python_callable=get_top_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len_name_domain',
                        python_callable=get_max_len_name_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

