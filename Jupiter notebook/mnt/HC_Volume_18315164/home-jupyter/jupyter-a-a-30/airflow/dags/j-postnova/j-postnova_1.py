# Updated 02/08/22 23:35
import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

#исходные данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#получение данных
def get_data():

    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# Найти топ-10 доменных зон по численности доменов
def get_zone(): 
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10_zone = top_doms.groupby('zone', as_index=False).agg('domain').count().rename(columns = {'domain' : 'zone_count'}).\
    sort_values('zone_count', ascending=False).head(10)
    return top_data_top_10_zone

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_domain():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['len'] = top_doms['domain'].str.len()
    top_len = top_doms.sort_values(['len', 'domain'], ascending = True)
    longest_domain = top_len.groupby('len', as_index=False).min().tail(1)
    return longest_domain
        
# На каком месте находится домен airflow.com?
def find_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    lookup = 'airflow.com'
    if top_doms["domain"].isin([lookup]).any():
        index = top_doms[top_doms['domain'] == lookup]
        find_data = index.iloc[0][0]
        print (index)
    else: 
        find_data = 'Was not found'
    return find_data


def print_data(ds):
    with open('top_data_top_10_zone.csv', 'r') as f:
        all_data_1 = f.read()
    with open('top_data_longest_domain.csv', 'r') as f:
        all_data_2 = f.read()
    date = ds
    with open('find_data.csv', 'r') as f:
        all_data_3 = f.read()
    date = ds

    print(f'Top zones for date {date}')
    print(all_data_1)

    print(f'Longest domain for date is {date}')
    print(all_data_2)
    
    print(f' Index of domain that is finding {date}')
    print(all_data_3)


default_args = {
    'owner': 'j.postnova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
}
schedule_interval = '0 12 * * *'

dag = DAG('Domains_j.postnova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zone',
                    python_callable=get_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='find_data',
                        python_callable=find_data,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
