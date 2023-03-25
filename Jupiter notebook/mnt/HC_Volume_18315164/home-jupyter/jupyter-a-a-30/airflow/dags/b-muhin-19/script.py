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


        
        
def get_top_10():
    df = pd.read_csv('top-1m.csv', index_col = 0, names=['rank', 'domain'])
    df['d_zone'] = df['domain'].str.split('.').str[-1]
    top_10 = df.groupby('d_zone', as_index=False).count().sort_values(by='domain', ascending=False).head(10)
    
    with open('top_10.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))


def get_len():
    df = pd.read_csv('top-1m.csv', index_col = 0, names=['rank', 'domain'])
    df['len'] = df['domain'].str.len()
    top_1_len = df.sort_values(by=['len', 'domain'], ascending=False).reset_index()['domain'].head(1)
    
    with open('top_1_len.csv', 'w') as f:
        f.write(top_1_len.to_csv(index=False, header=False))


def aiflow_rank():
    df = pd.read_csv('top-1m.csv', index_col = 0, names=['rank', 'domain'])
    
    with open('aiflow_rank.csv', 'w') as f:
        if df.query('domain == "airflow"').empty:
            f.write('airflow.com is not in top')
        else:
            airflow_rank = df.query('domain == "airflow"')['rank']
            f.write(airflow_rank)


            
            
def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('top_1_len.csv', 'r') as f:
        top_1_len = f.read()
    with open('aiflow_rank.csv', 'r') as f:
        aiflow_rank = f.read()
        
    date = ds

    print(f'Top 10 domains in world for date {date}')
    print(top_10)

    print(f'The longest domain for date {date}')
    print(top_1_len)
    
    print(f'Airflow rank if exist {date}')
    print(aiflow_rank)


default_args = {
    'owner': 'b-muhin-19',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 7),
}
schedule_interval = '0 12 * * *'

dag = DAG('b-muhin-19_top_10', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='top_10',
                    python_callable=get_top_10,
                    dag=dag)

t2_len = PythonOperator(task_id='top_1_len',
                        python_callable=get_len,
                        dag=dag)

t2_airflow = PythonOperator(task_id='aiflow_rank',
                        python_callable=aiflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_len, t2_airflow] >> t3