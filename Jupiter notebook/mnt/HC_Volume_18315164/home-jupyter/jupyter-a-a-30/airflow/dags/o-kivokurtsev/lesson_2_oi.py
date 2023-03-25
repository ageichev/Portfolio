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
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone_top'] = df.domain.apply(lambda x: x.split('.')[-1])
    df_top_10_zone= (df.groupby('zone_top')
         .domain.count()
         .sort_values(ascending=False)
         .to_frame().head(10))
    
    with open('df_top_10_zone.csv', 'w') as f:
        f.write(df_top_10_zone.to_csv(header=False))


def get_longest():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['len_name'] = df['domain'].apply(lambda x: len(x.split('.')[0]))
    max_len_name = df.sort_values(['len_name', 'domain'], ascending={False, True}) \
                        [['domain', 'len_name']] \
                        .head(1)
    
    with open('max_len_name.csv', 'w') as f:
        f.write(max_len_name.to_csv(index=False, header=False)) 
        
def airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        
    if df.query("domain == 'airflow.com'").shape[0] != 0:
        airflow_rank = df.query("domain == 'airflow.com'")
    else:
        airflow_rank = pd.DataFrame({'rank': 'not found', 'domain': ['airflow.com']})

    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))
        
def print_data(ds):
    with open('df_top_10_zone.csv', 'r') as f:
        df_top_10_zone = f.read()
    with open('max_len_name.csv', 'r') as f:
        max_len_name = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()    
    date = ds

    print(f'Top domains zone by domain counts for date {date}')
    print(df_top_10_zone)

    print(f'The longest domail name for date {date} is {max_len_name}')
    print(f'Airfow.com rank for date {date} is {airflow_rank}')
    
    
default_args = {
    'owner': 'o.kivokurtsev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
    'start_date': datetime(2022, 8, 24),
}
schedule_interval = '0 12 * * *'
schedule_interval = '@daily'

dag = DAG('top_10_ru_kivokurtsev_oi', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top',
                    python_callable=get_top,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)
t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
