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

def top_ten_by_domain():
    top_ten_by_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_ten_by_domain['zone'] = top_ten_by_domain.domain.str.split('.').str[-1]
    top_ten = top_ten_by_domain.groupby('zone', as_index=False).agg({'domain':'count'}).sort_values('domain', ascending=False).head(10)
    with open('top_ten.csv', 'w') as f:
        f.write(top_ten.to_csv(index=False, header=False))

def longest_name():
    longest_name = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_name['name_lenght'] = longest_name.domain.str.len()
    ln = longest_name.sort_values('name_lenght', ascending=False).head(1).domain
    with open('longest_name.csv', 'w') as f:
        f.write(ln.to_csv(index=False, header=False))
    
def airflow_position():
    airflow_position = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        position = f'''Airflow position is {airflow_position.query('domain == "airflow.com"')['rank'].to_list()[0]}'''
    except:
        position = pd.DataFrame(['Airflow is not in list'])
    with open('airflow_position.csv', 'w') as f:
        f.write(position.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_ten.csv', 'r') as f:
        top_ten_by_domain = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_position = f.read()
    date = ds

    print(f'Top 10 domains by amount for date {date}')
    print(top_ten_by_domain)

    print(f'TLongest domain for date {date}')
    print(longest_name)

    print(f'Airflow position on date {date}')
    print(airflow_position)

default_args = {
    'owner': 'a.zorenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '0 3 * * *'

dag = DAG('zorenko_lesson_2_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_ten_by_domain',
                    python_callable=top_ten_by_domain,
                    dag=dag)

t3 = PythonOperator(task_id='longest_name',
                        python_callable=longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_position',
                        python_callable=airflow_position,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5