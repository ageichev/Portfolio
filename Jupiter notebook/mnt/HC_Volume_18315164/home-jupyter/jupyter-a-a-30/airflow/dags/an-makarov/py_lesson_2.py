import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])

#Напишем функцию которая считает длину домена и применим его к нашаему датафрейму

def len_domain(domain):
    return len(domain)

def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_data_df = pd.read_csv(TOP_1M_DOMAINS)
    top_data_df = top_data_df.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data_df)
        
def get_top_domains():
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: ".".join([i for i in x.split('.')[1:] if i]))
    top_10_domains = top_data_df.groupby('domain_zone', as_index=False).agg({'domain':'count'}) \
    .sort_values('domain', ascending=False).head(10) \
    .rename(columns = {'domain': 'quantity'})
    
    
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))        
        
def get_len_max():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df['domain'].apply(len_domain)
    #Найдем максимальную длину и сравним с остальными, если таковые имеются при наличии двух максимальных длин
    max_len = top_data_df['len_domain'].max()
    max_len_domain = top_data_df[top_data_df.len_domain >= max_len]
    
    with open('max_len_domain.csv', 'w') as f:
        f.write(max_len_domain.to_csv(index=False, header=False))   
        
def get_position_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        airflow_index = df[df.domain == 'airflow.com'].index[0]
    except:
        airflow_index = 'Entry not found.'
    with open('airflow.txt', 'w') as f:
        f.write(airflow_index)

def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        top_10_domains = f.read()
    with open('max_len_domain.csv', 'r') as f:
        max_len_domain = f.read()
    with open('airflow_index.txt', 'r') as f:
        airflow_index = f.read()    
    date = ds

    print(f'Top domains in for date {date}')
    print(top_10_domains)

    print(f'max len of all domains for date {date}')
    print(max_len_domain)
    
    print(f'number index of airflow.com {date}')
    print(airflow_index)            

default_args = {
    'owner': 'a.makarov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 21),
}
schedule_interval = '0 22 * * *'

dag = DAG('lesson_2_makarovs', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_len_max',
                        python_callable=get_len_max,
                        dag=dag)

t4 = PythonOperator(task_id='get_position_airflow',
                    python_callable=get_position_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
    
