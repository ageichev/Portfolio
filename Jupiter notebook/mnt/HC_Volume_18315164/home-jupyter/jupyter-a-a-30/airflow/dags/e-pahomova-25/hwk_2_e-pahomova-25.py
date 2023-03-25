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

def get_top_domains():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['top_domains'] = df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domains = df.groupby('top_domains', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10)
    with open('top_domains.csv', 'w') as f:
        f.write(top_domains.to_csv(index=False, header=False))

def get_domain_longest_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df['domain'].str.len()
    domain_length = df.sort_values('length', ascending=False).head(1).domain
    with open('domain_longest_name.csv', 'w') as f:
        f.write(domain_longest_name.to_csv(index=False, header=False))
    
def get_airflow_order():
    df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    airflow_order = df.query('domain == "airflow.com"')['rank'].values[0]
    with open('airflow_order.csv', 'w') as f:
        f.write(airflow_order)    

def print_data(ds):
    with open('top_domains.csv', 'r') as f:
        top_domains = f.read()
    with open('domain_longest_name.csv', 'r') as f:
        domain_longest_name = f.read()
    with open('airflow_order.csv', 'r') as f:
        airflow_order = f.read()
    date = ds

    print(f'The most popular domains for date {date}')
    print(top_domains)

    print(f'Domain with the longest name for date {date}')
    print(domain_longest_name)
    
    print(f'Airflow.com order for date {date}')
    print(airflow_order)


default_args = {
    'owner': 'e-pahomova-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 31),
}
schedule_interval = '0 10 * * *'

dag = DAG('hw_2_e-pahomova-25', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_domain_longest_name',
                        python_callable=get_domain_longest_name,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_airflow_order',
                        python_callable=get_airflow_order,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3

#t1.set_downstream(t2_1)
#t1.set_downstream(t2_2)
#t1.set_downstream(t2_3)
#t2_1.set_downstream(t3)
#t2_2.set_downstream(t3)
#t2_3.set_downstream(t3)