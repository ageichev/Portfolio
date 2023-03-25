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


def top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    
    top_10_domain = top_data_df.groupby('domain_zone', as_index=False).agg({'domain': 'count'})
    top_10_domain.rename(columns={'domain': 'count_domain'}).sort_values('domain_count', ascending=False).head(10) 


    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))  

        
def max_len_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domen_name'] = top_data_df['domain'].apply(lambda x: len(x.split('.')[0]))
    
    max_domen_name_len = top_data_df.sort_values(['len_domen_name', 'domain'], ascending={False, True})
    max_domen_name_len.drop(['len_domen_name', 'rank'], axis=1).head(1)
    with open('max_len_name.csv', 'w') as f:
        f.write(max_domen_name_len.to_csv(index=False, header=False))        

def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    if top_data_df.query("domain == 'airflow.com'").shape[0] != 0:
        rank_airflow = top_data_df.query("domain == 'airflow.com'")
    else:
        rank_airflow = pd.DataFrame({'rank': 'not found', 'domain': ['airflow.com']})

    with open('airflow_rank.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False)) 

def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        top_10_tmp = f.read()
    with open('max_len_name.csv', 'r') as f:
        max_len_tmp = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_tmp = f.read()
    date = ds

    print(f'Top-10 domain zone:  {date}')
    print(top_10_tmp)

    print(f'Max len domain name:  {date}')
    print(max_len_tmp)
    
    print(f'Airflow rank:  {date}')
    print(airflow_rank_tmp)

    

default_args = {
    'owner': 'v-kachanovskij-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 4),
    
}
schedule_interval = '0 12 * * *'

dag = DAG('v-kachanovskij-24_lesson02', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='max_len_name',
                    python_callable=max_len_name,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=rank_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_results',
                    python_callable=print_results,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
