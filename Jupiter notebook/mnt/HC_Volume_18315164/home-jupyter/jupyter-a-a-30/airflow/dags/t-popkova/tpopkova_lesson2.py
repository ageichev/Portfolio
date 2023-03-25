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
        
        
        
def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_zones = top_data_df
    top_10_zones['zone'] = top_10_zones.domain.apply(lambda x: x.rsplit('.')[1])
    top_10_zones = top_10_zones.groupby('zone', as_index=False)\
                .agg({'domain':'count'})\
                .sort_values('domain', ascending=False)\
                .head(10)\
                .reset_index()\
                .zone
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))

        
def get_long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    long_domain = top_data_df
    long_domain['len_domain'] = long_domain.domain.apply(lambda x: len(x))
    long_domain = long_domain[['domain','len_domain']].drop_duplicates()\
                                                .sort_values('len_domain',ascending=False)\
                                                .sort_values('domain')\
                                                .head(1)\
                                                .reset_index()\
                                                .len_domain
    with open('long_domain.csv', 'w') as f:
        f.write(long_domain.to_csv(index=False, header=False))
        
def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_airflow = top_data_df.query('domain=="airflow.com"')['rank']
    if rank_airflow.shape[0] == 1:
        with open('rank_airflow.csv', 'w') as f:
            f.write(rank_airflow.to_csv(index=False, header=False))
    else:
        rank_airflow = 'No information'
        with open('rank_airflow.txt', 'w') as f:
            f.write(rank_airflow) 
        


def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('long_domain.csv', 'r') as f:
        long_domain = f.read()
    with open('rank_airflow.csv', 'r') as f:
        rank_airflow = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_zones)

    print(f'Longest domain for date {date}')
    print(long_domain)
    
    print(f'Rank airflow.com for date {date}')
    print(rank_airflow)


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 23),
}
schedule_interval = '0 21 * * *'

dag = DAG('tpopkova_domen_info', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_domain',
                        python_callable=get_long_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                    python_callable=get_rank_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
