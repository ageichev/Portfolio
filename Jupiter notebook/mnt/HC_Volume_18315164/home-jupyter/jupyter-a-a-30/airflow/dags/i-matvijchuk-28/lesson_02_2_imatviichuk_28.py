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
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_10_zones = df.zone.value_counts().head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(header=False))
        
        
def get_max_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.apply(lambda x: len(x))
    top_data_max_length = top_data_df.sort_values(['length', 'domain'], ascending=[False,True]).head(1)
    with open('top_data_max_length.csv', 'w') as f:
        f.write(top_data_max_length.to_csv(index=False, header=False))
        
        
def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_airflow = top_data_df.query("domain == 'airflow.com'")
    with open('top_data_airflow.csv', 'w') as f:
        f.write(top_data_airflow.to_csv(index=False, header=False))        
        

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top10 = f.read()
    with open('top_data_max_length.csv', 'r') as f:
        max_length = f.read()
    with open('top_data_airflow.csv', 'r') as f:
        airflow = f.read()        
    date = ds

    print(f'Top domains for date {date}')
    print(top10)

    print(f'Domain with max length for date {date}')
    print(max_length)

    print(f'Rank for airflow.com for date {date}')
    print(airflow)   


default_args = {
    'owner': 'i.matviichuk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 10),
}
schedule_interval = '30 10 * * *'

dag = DAG('imatviichuk_lesson2_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length',
                    python_callable=get_max_length,
                    dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                    python_callable=get_rank_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)