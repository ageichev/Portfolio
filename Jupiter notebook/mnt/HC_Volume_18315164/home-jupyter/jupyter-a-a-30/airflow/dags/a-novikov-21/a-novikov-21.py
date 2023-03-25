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

def get_top_10():
    top_10_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_df['zone'] = top_10_df.apply(lambda x: x['domain'].split('.')[-1], axis=1)
    data_top_10 = top_10_df.groupby('zone', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10)
    with open('data_top_10.csv', 'w') as f:
        f.write(data_top_10.to_csv(index=False, header=False))

def get_longest_name():
    longest_name_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_name_df['length_name'] = longest_name_df.apply(lambda x: len(x.domain), axis=1) 
    longest_name = longest_name_df.sort_values(['length_name','domain'], ascending=False).iloc[0].domain 
    with open('longest_name.txt', 'w') as f:
        f.write(longest_name)
                       
def get_airflow_position():
    airflow_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_position = airflow_df.query('domain == "airflow.com"')['rank']
    with open('airflow_position.txt', 'w') as f:
        f.write(str(airflow_position))

def print_data(ds):
    with open('data_top_10.csv', 'r') as f:
        data_top_10 = f.read()
    with open('longest_name.txt', 'r') as f:
        longest_name = f.read()
    with open('airflow_position.txt', 'r') as f:
        airflow_position = f.read()
    date = ds

    print(f'Top 10 domains zones for {date}')
    print(data_top_10)

    print(f'Longest name of domain for {date}')
    print(longest_name)

    print(f'Airflow position for {date} is')
    print(airflow_position)




default_args = {
    'owner': 'a-novikov-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 19),
}
schedule_interval = '0 10 * * *'

dag = DAG('a-novikov-21', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)
                       
t2_longest = PythonOperator(task_id='get_longest_name',
                    python_callable=get_longest_name,
                    dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_position',
                    python_callable=get_airflow_position,
                    dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_longest, t2_airflow] >> t3

