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

def get_zone_stat():
    top_10_zones = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_zones['zone'] = top_10_zones['domain'].apply(lambda x: ".".join([c for c in x.split('.')[1:] if c]))
    top_10_zones = top_10_zones.zone.value_counts().to_frame().reset_index().head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))

def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_name = top_data_df[top_data_df['domain'].str.len() == top_data_df['domain'].str.len().max()]
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df.domain == 'airflow.com']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        zone_data = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_domain_name = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rnk = f.read()    
    date = ds

    print(f'Top zones for date {date}')
    print(zone_data)

    print(f'Longest domain name for date {date}')
    print(longest_domain_name)
    
    print(f'Airflow rank for date {date}')
    if len(airflow_rnk) == 0:
        print('Sorry, no such domain found in the rating')
    else:
        print(airflow_rnk)


default_args = {
    'owner': 'a.gorbanev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 15),
}
schedule_interval = '0 8 * * *'

dag = DAG('a.gorbanev_lesson_02', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zone_stat',
                    python_callable=get_zone_stat,
                    dag=dag)

t2_ln = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t2_ar = PythonOperator(task_id='airflow_rnk',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_ln, t2_ar] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)