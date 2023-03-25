# updated file
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


def get_top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'page'])
    top_data_df['domain'] = top_data_df.page.apply(lambda x: x.rsplit('.', 1)[-1])
    top_10_domains = top_data_df.groupby('domain', as_index=False).agg({'page': 'count'})\
    .rename(columns = {'page': 'amount'})\
    .sort_values('amount', ascending = False)\
    .head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))


def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'page'])
    top_data_df['name_length'] = top_data_df.page.str.len()
    max_length_name = top_data_df.sort_values('name_length', ascending=False).head(1).page
    with open('max_length_name.csv', 'w') as f:
        f.write(max_length_name.to_csv(index=False, header=False))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'page'])
    
    with open('domain_rank.csv', 'w') as f:
        if top_data_df[top_data_df['page']=='airflow.com'].empty:
            f.write('There is no such domain')
        else:
            x = top_data_df[top_data_df['page']=='airflow.com']['rank']
            f.write(x)
        
        
def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        top_10_domains_data = f.read()
    with open('max_length_name.csv', 'r') as f:
        max_length_name_data = f.read()
    with open('domain_rank.csv', 'r') as f:
        domain_rank_data = f.read()
    date = ds

    print(f'Top 10 domain names for date {date}')
    print(top_10_domains_data)

    print(f'The longest domain name for date {date}')
    print(max_length_name_data)
    
    print(f'Domain rank {date}')
    print(domain_rank_data)

default_args = {
    'owner': 'v-ivanova-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 31),
}
schedule_interval = '0 8 * * *'

dag = DAG('v-ivanova-22_top_domains', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    dag=dag)

t2_len = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_len, t2_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)