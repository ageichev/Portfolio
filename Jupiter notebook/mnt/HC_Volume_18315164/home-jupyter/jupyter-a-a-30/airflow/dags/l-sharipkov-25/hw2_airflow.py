import requests
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

def get_top_domain_zone():
    top_10_zone = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_10_zone['zone'] = top_10_zone.domain.str.split('.').str[-1]
    top_10_zone = top_10_zone.groupby('zone',as_index=False).domain.count().sort_values('domain',ascending=False).head(10)
    
    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10_zone.to_csv(index=False, header=False))

def get_longest_domain():
    data = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    max_lenght = data.domain.str.len().sort_values(ascending=False).head(1).values[0]
    data.loc[data.domain.str.len() == max_lenght]
    data = data.loc[data.domain.str.len() == max_lenght]['domain'].values[0]
    
    with open('longest_domain.txt', 'w') as f:
        f.write(data)

def get_airflow():
    try:
        data = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
        airflow_rank = data.query('domain == "airflow.com"')['rank'].values[0]
    except:
        airflow_rank = 'No domain'
    
    with open('airflow_com.txt', 'w') as f:
        f.write(airflow_rank)


def print_data(ds):
    with open('top_10_zone.csv', 'r') as f:
        top_10_zone = f.read()
        
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
        
    with open('airflow_com.txt', 'r') as f:
        airflow_com = f.read()
        
    date = ds

    print(f'Top 10 domain zones by number of domains for date {date}')
    print(top_10_zone)

    print(f'The longest domain for date {date}')
    print(longest_domain)
    
    print(f'The rank of airflow.com for date {date}')
    print(airflow_com)

default_args = {
    'owner': 'l.sharipkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 13),
}
schedule_interval = '0 12 * * *'

dag = DAG('l.sharipkov-25', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_domain_zone = PythonOperator(task_id='get_top_domain_zone',
                    python_callable=get_top_domain_zone,
                    dag=dag)

t2_longest_domain = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_get_rank = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_domain_zone, t2_longest_domain, t2_get_rank] >> t3