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

def dom_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    df_top_dom = top_data_df.groupby('domain_zone', as_index=False).agg({'domain':'count'})\
                .sort_values(by='domain', ascending=False).rename(columns = {'domain' : 'count'})
    df_top_dom = df_top_dom.head(10).drop('count', axis=1)

    with open('top10_domain_zone.csv', 'w') as f:
        f.write(df_top_dom.to_csv(index=False, header=False))
    
def dom_long_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name'] = top_data_df['domain'].str.split('.').str[-2]
    df_long_name = top_data_df[top_data_df['domain_name'].str.len() == top_data_df['domain_name'].str.len().max()]\
                .sort_values(by='domain_name')
    df_long_name = df_long_name.head(1).drop(['domain_name', 'rank'], axis=1)
    
    with open('top1_domain_long_name.csv', 'w') as f:
        f.write(df_long_name.to_csv(index=False, header=False))
        
def get_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow=top_data_df[top_data_df['domain'] == 'airflow.com']

    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))
        
def print_data(ds):

    with open('top10_domain_zone.csv', 'r') as f:
        all_data = f.read()

    with open('top1_domain_long_name.csv', 'r') as f:
        all_data_len = f.read()

    with open('airflow.csv', 'r') as f:
        all_data_name = f.read()

    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)

    print(f'The longest domain name  {date}')
    print(all_data_len)
       
    print(f'The rank of airflow.com {date}')
    print(all_data_name)
    
default_args = {
    'owner': 'a.abdibekova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 22),
}

schedule_interval = '0 12 * * *'

dag = DAG('a-malikov-lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='dom_zone',
                    python_callable=dom_zone,
                    dag=dag)

t2_name = PythonOperator(task_id='dom_long_name',
                        python_callable=dom_long_name,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_name',
                        python_callable=get_name,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_name, t2_airflow] >> t3