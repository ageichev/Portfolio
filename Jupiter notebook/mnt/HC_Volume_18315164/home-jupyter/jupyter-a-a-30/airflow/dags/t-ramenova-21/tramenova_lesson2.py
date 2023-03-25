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

def get_top_zone_10():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    #взять от имени сайта только зону
    data_df.domain = data_df.domain.apply(lambda c: c.split('.')[-1])
    
    #сгруппировать по зонам, посчитать количество,  отсортировать по убыванию, взять топ 10 
    top_data_zone_10 = data_df.groupby('domain', as_index = False)\
                        .agg({'rank': 'count'})\
                        .sort_values('rank', ascending= False)\
                        .head(10)
    
    with open('tramenova_top_data_dom_10.csv', 'w') as f:
        f.write(top_data_zone_10.to_csv(index=False, header=False))

def get_max_len_dom():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['len'] =  data_df.domain.str.len() 
    top_len_domain = data_df.sort_values(['len', 'domain'], ascending= [False, True]).head(1)
    
    with open('tramenova_max_len_dom.csv', 'w') as f:
        f.write(top_len_domain.to_csv(index=False, header=False))

def get_airflow_rank():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = data_df[data_df.domain == 'airflow.com']
    with open('tramenova_airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('tramenova_top_data_dom_10.csv', 'r') as f:
        top_zone_data = f.read()
    with open('tramenova_max_len_dom.csv', 'r') as f:
        max_len_dom = f.read()
    with open('tramenova_airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()    
    date = ds

    print(f'Top zones by domains for date {date}')
    print(top_zone_data)

    print(f'Max domain name for date {date}')
    print(max_len_dom)
    
    print(f'Rank of airflow.com for date {date}')
    print(airflow_rank)

default_args = {
    'owner': 't.ramenova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 29),
}
schedule_interval = '0 6 * * *'

dag = DAG('tramenova_task2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_zone_10',
                    python_callable=get_top_zone_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len_dom',
                        python_callable=get_max_len_dom,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5











