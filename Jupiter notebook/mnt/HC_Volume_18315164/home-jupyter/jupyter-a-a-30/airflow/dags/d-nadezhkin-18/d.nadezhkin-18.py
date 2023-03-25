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


#def get_stat():
#    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#    top_data_top_10 = top_data_top_10.head(10)
#    with open('top_data_top_10.csv', 'w') as f:
#        f.write(top_data_top_10.to_csv(index=False, header=False))


#def get_stat_com():
#    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#    top_data_top_10 = top_data_top_10.head(10)
#    with open('top_data_top_10_com.csv', 'w') as f:
#        f.write(top_data_top_10.to_csv(index=False, header=False))


def top_10_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['top'] = top_data_df.domain.str[-3:]                           # берем последние 3 символа строки
    top_10_domains = top_data_df.groupby('top', as_index=False)\
                                .agg({'rank':'count'})\
                                .sort_values('rank', ascending=False)\
                                .head(10)\
                                .top\
                                .str.replace('.','')
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))


def top_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.apply(lambda x: len(x))
    top_len_df = top_data_df.sort_values('len', ascending=False).head(1)['domain']
    with open('top_len_df.csv', 'w') as f:
        f.write(top_len_df.to_csv(index=False, header=False))
    
    
def position_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    position_airflow = top_data_df[top_data_df['domain'].str.startswith('airflow')]
    with open('position_airflow.csv', 'w') as f:
        f.write(position_airflow.to_csv(index=False, header=False))
    
    
def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        all_data = f.read()
    with open('top_len_df.csv', 'r') as f:
        all_data_len = f.read()
    with open('position_airflow.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top domains in world for date {date}')
    print(all_data)

    print(f'Top len domain for date {date}')
    print(all_data_len)
    
    print(f'airflow site position {date}')
    print(all_data_airflow)


default_args = {
    'owner': 'd.nadezhkin-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 3, 29),
}
schedule_interval = '30 8 * * *'

dag = DAG('d_nadezhkin_18', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_domain = PythonOperator(task_id='top_10_domain',
                    python_callable=top_10_domain,
                    dag=dag)

t2_top_len_domain = PythonOperator(task_id='top_len_domain',
                        python_callable=top_len_domain,
                        dag=dag)

t2_position_airflow = PythonOperator(task_id='position_airflow',
                    python_callable=position_airflow,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_domain, t2_top_len_domain, t2_position_airflow] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)