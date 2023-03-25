import requests
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


def get_zone_top_10_us():
    top_data_us = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_us['domain_zone']=top_data_us.domain.str.split('.').str[-1]    
    top_domain_10_us=top_data_us.domain_zone.value_counts().sort_values(ascending=False).reset_index().head(10)
    top_domain_10_us.rename(columns = {'index' : 'domain', 'domain_zone':'quantity'}, inplace = True)
    with open('top_domain_10_us.csv', 'w') as f:
        f.write(top_domain_10_us.to_csv(index=False, header=False))


def get_max_length_domain_us():
    top_data_us = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_us['domain_name'] = top_data_us['domain'].str.split('.').str[-2]
    top_data_us['domain_name'].astype('str')
    top_data_us['length']=top_data_us['domain_name'].map(lambda x: len(x))
    max_length_domain_us=top_data_us.sort_values(by=['length'],ascending=False).head(1)
    with open('max_length_domain_us.txt', 'w') as f:
        f.write(str(max_length_domain_us))

def domain_rank_us():
    top_data_us = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_us['domain_name'] = top_data_us['domain'].str.split('.').str[-2]
    place_us=top_data_us.query('domain_name=="airflow"')
    if place_us.empty:
        print('not_found')
    else:
        print(top_data_us.rank)
    with open('place_us.txt', 'w') as f:
        f.write(str(place_us))
        
def print_data(ds):
    with open('top_domain_10_us.csv', 'r') as f:
        all_data = f.read()
    with open('max_length_domain_us.txt', 'r') as f:
        all_data_len = f.read()
    with open('place_us.txt', 'r') as f:
        all_data_name = f.read()    
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)
    print(f'The longest domain name for date {date}')
    print(all_data_len)
    print(f'The rank of airflow.com for date {date}')
    print(all_data_name)

default_args = {
    'owner': 'a.usacheva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=7),
    'start_date': datetime(2022, 7, 29),
}
schedule_interval = '10 12 * * *'

dag = DAG('a-usacheva', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2 = PythonOperator(task_id='get_zone_top_10_us',
                    python_callable=get_zone_top_10_us,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length_domain_us',
                        python_callable=get_max_length_domain_us,
                        dag=dag)

t4 = PythonOperator(task_id='domain_rank_us',
                    python_callable=domain_rank_us,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3,t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

