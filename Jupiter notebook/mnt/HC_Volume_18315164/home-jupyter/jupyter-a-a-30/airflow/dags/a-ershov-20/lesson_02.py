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
        
        
def frequent_domains_top10():    
    domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain'])
    domains_df['d_zone'] = domains_df.domain.apply(lambda x: x.split('.')[-1]) # создаем колонку с доменными зонами
    frequency_domains = domains_df.groupby('d_zone', as_index=False) \
                                                .count() \
                                                .sort_values('domain', ascending=False) \
                                                .head(10)
    
    return frequency_domains


def long_dom():
    dom_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain']) 
    dom_df['na_domain'] = dom_df.domain.apply(lambda x: x.split('.')[-2]) # создаем колонку с доменнами  

    dom_df['domain_long'] = dom_df.na_domain.apply(lambda x: len(x)) # находим длинные доменны

    max = dom_df.domain_long.max()
    
    long_domain_all = sorted(dom_df.query('domain_long == @max') \
                         .na_domain.to_list())
    return long_domain_all



def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_top_10_com.csv', 'r') as f:
        all_data_com = f.read()
    date = ds

    print(f'Top domains in .RU for date {date}')
    print(all_data)

    print(f'Top domains in .COM for date {date}')
    print(all_data_com)


default_args = {
    'owner': 'a-ershov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 25),
}
schedule_interval = '0 12 * * *'

dag = DAG('a-ershov-20', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=frequent_domains_top10,
                    dag=dag)

t2_com = PythonOperator(task_id='get_stat_com',
                        python_callable=long_dom,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_com] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)