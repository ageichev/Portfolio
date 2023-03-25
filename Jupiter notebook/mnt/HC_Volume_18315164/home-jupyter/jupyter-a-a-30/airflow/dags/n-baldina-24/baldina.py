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


def get_top():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df.sort_values(by='rank', ascending = False)
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.str.len()
    longest = top_data_df.sort_values(by='domain').sort_values('domain_len', ascending=False)
    longest = longest.head(1)
    with open('longest.csv', 'w') as f:
        f.write(longest.to_csv(index=False, header=False))

def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = top_data_df.query('domain=="airflow.ru"')
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('longest.csv', 'r') as f:
        all_data_longest = f.read()
    with open('airflow.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top 10 domains by {date}')
    print(all_data)

    print(f'The longest name by {date}')
    print(all_data_longest)
    
    print(f'place of airflow.ru by {date}')
    print(all_data_airflow)


default_args = {
    'owner': 'n-baldina-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 3),
}
schedule_interval = '0 12 * * *'

nbaldina_dag = DAG('n-baldina-24', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=nbaldina_dag)

t2 = PythonOperator(task_id='get_top',
                    python_callable=get_top,
                    dag=nbaldina_dag)

t3 = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=nbaldina_dag)

t4 = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=nbaldina_dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=nbaldina_dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)