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
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def get_top_10lvl_domain():
    top_10lvl_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10lvl_domain['top_lvl_domain'] = top_10lvl_domain.domain.str.split('.').str[1]   
    top_10lvl_domain = top_10lvl_domain.groupby("top_lvl_domain", as_index=False) \
                       .agg({"rank": "count"}) \
                       .sort_values("rank", ascending=False) \
                       .reset_index()[['top_lvl_domain']].head(10)       
    with open('top_10lvl_domain.csv', 'w') as f:
        f.write(top_10lvl_domain.to_csv(index=False, header=False))

def get_longest_domain():
    longest_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain['name'] = longest_domain.domain.str.split('.').str[0]
    longest_domain['length_name'] = longest_domain.name.apply(lambda x: len(x))
    longest_domain = longest_domain.sort_values('length_name', ascending=False).head(1).name
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))
        
def get_airflow_position():
    airflow_position = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_position = airflow_position.query('domain == "airflow.com"')
    if airflow_position.empty == False:
        airflow_position = airflow_position['rank']
    else:
        airflow_position = 'No domain.'    
        with open('airflow_position.csv', 'w') as f:
            f.write(airflow_position)
    
def print_data(ds):
    with open('top_10lvl_domain.csv', 'r') as f:
        top_10lvl_domain = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_position = f.read()    
    date = ds

    print(f'Top 10 domain zones {date}')
    print(top_10lvl_domain)

    print(f'Longest domain name {date}')
    print(longest_domain)
   
    print(f'Rank airflow.com {date}')
    print(airflow_position)

default_args = {
    'owner': 'i-ishakov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 23),
}

schedule_interval = '* 12 * * *'

dag = DAG('i-ishakov-22', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10lvl_domain',
                    python_callable=get_top_10lvl_domain,
                    dag=dag)    

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_position',
                    python_callable=get_airflow_position,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5