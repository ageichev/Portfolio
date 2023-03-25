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

def get_top_10_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone']=top_data_df.domain.apply(lambda x:x.split('.')[1])
    top_10_zones=top_data_df.zone.value_counts()[:10]
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=True, header=False))

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.apply(len)
    longest_domain=top_data_df.sort_values(['len','domain'],ascending=[False,True]).reset_index()[:1]
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))
    
def find_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = top_data_df.query("domain.str.contains('airflow.com')")['rank']
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow.csv', 'r') as f:
        airflow = f.read()
    date = ds

    print(f'Top zones by domain count for date {date}')
    print(top_10_zones)

    print(f'Longest domain for date {date}')
    print(longest_domain)
    
    print(f'Domains containing "airflow.com" ranks for date {date}')
    print(airflow)


default_args = {
    'owner': 'm_mezhueva_19',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 12),
}
schedule_interval = '0 12 * * *'

dag = DAG('m_mezhueva_19', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t4 = PythonOperator(task_id='find_airflow',
                    python_callable=find_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

