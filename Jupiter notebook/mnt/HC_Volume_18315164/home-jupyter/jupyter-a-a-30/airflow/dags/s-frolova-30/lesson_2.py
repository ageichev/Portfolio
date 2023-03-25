import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_zones = top_data_df.groupby('zone', as_index=False) \
        .agg({'domain': 'count'}) \
        .sort_values('domain', ascending=False)
    top_zones = top_zones[['zone', 'domain']].head(10)
    with open('top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))


def get_stat_names():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['lenth']=top_data_df['domain'].apply(lambda x: len(x))
    top_names = top_data_df.sort_values(['lenth', 'domain'], ascending= [False, True])
    top_names = top_names.domain.head(1)
    with open('top_names.csv', 'w') as f:
        f.write(top_names.to_csv(index=False, header=False))    
        
def get_stat_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['lenth']=top_data_df['domain'].apply(lambda x: len(x))
    df_airflow = top_data_df.sort_values(['lenth', 'domain'], ascending= [False, True])
    df_airflow = df_airflow.reset_index(drop=True)
    df_airflow = df_airflow.query('domain == "airflow.com"')
    df_airflow = df_airflow.reset_index()
    df_airflow = df_airflow[['index', 'domain']]
    with open('df_airflow.csv', 'w') as f:
        f.write(df_airflow.to_csv(index=False, header=False))
       
        
def print_data(ds): 
    with open('top_zones.csv', 'r') as f:
        all_data_zones = f.read()
    with open('top_names.csv', 'r') as f:
        all_data_names = f.read()
    with open('df_airflow.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(all_data_zones)

    print(f'Longest domain name for date {date}')
    print(all_data_names)
    
    print(f'Airflow domain rank for date {date}')
    print(all_data_airflow)


default_args = {
    'owner': 's-frolova-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 2, 20),
}
schedule_interval = '0 12 * * *'

dag = DAG('s-frolova-30_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_zones',
                    python_callable=get_stat_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_stat_names',
                        python_callable=get_stat_names,
                        dag=dag)

t4 = PythonOperator(task_id='get_stat_airflow',
                        python_callable=get_stat_airflow,
                        dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

