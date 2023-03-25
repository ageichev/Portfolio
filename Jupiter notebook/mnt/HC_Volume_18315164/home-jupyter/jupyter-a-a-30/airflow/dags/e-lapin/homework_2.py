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

def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_df10 = top_data_df.zone.value_counts().head(10).to_frame().reset_index()
    
    with open('top_data_df10.csv', 'w') as f:
        f.write( top_data_df10.to_csv(index=False, header=False))

def get_stat_max_dom_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].str.len()
    top_data_df.loc[top_data_df['length'].idxmax(),'domain']
        
    with open('max_dom_len.txt', 'w') as f:
        f.write(top_data_df.loc[top_data_df['length'].idxmax(),'domain'])

def get_stat_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank_number', 'domain'])
    site = 'airflow.com'

    if top_data_df["domain"].isin([site]).any():
        x = top_data_df\
            .loc[top_data_df.domain == 'airflow.com']\
            .rank_nubmer.values[0]
    else:
        x = 'Домен airflow.com не найден в рейтинге Alexa'

    with open('airflow_rank.txt','w') as f:
        f.write(x)

def print_data(ds):
    with open('top_data_df10.csv', 'r') as f:
        top_data_df10 = f.read()
        
    with open('max_dom_len.txt', 'r') as f:
        max_dom_len = f.read()
        
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()    
        
    date = ds

    print(f'топ-10 доменных зон по численности доменов {date}')
    print( top_data_df10)

    print(f'Домен с самым длинным именем {date}')
    print(max_dom_len, end='\n')
    
    print(f'Ранк домена airflow {date}')
    print(airflow_rank)   

default_args = {
    'owner': 'e-lapin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 23),
}
schedule_interval = '0 12 * * *'

dag = DAG('airflow_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)

t3 = PythonOperator(task_id='get_stat_max_dom_len',
                        python_callable=get_stat_max_dom_len,
                        dag=dag)

t4 = PythonOperator(task_id='get_stat_airflow_rank',
                    python_callable=get_stat_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5