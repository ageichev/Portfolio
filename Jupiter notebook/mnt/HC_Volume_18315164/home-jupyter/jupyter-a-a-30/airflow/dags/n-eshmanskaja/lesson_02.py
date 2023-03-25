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

def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone']=top_data_df.domain.str.split('.').str[-1]    
    top_domain_10=top_data_df.domain_zone.value_counts().sort_values(ascending=False).reset_index().head(10)
    top_domain_10.rename(columns = {'index' : 'domain', 'domain_zone':'count'}, inplace = True)
    with open('top_domain_10.csv', 'w') as f:
        f.write(top_domain_10.to_csv(index=False, header=False))


def max_len_domain():
    max_len_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len_domain['domain_len'] = max_len_domain['domain'].apply(lambda x: len(x))
    max_len_domain = max_len_domain.sort_values(['domain_len', 'domain'], ascending = [False, True]).iloc[0].domain
    with open('max_len_domain.txt', 'w') as f:
        f.write(str(max_len_domain))

def get_domain_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name'] = top_data_df['domain'].str.split('.').str[-2]
    domain_place=top_data_df.query('domain_name=="airflow"')
    if domain_place.empty:
        print('not_found')
    else:
        print(top_data_df.rank)
    with open('domain_place.txt', 'w') as f:
        f.write(str(domain_place))
        

def print_data(ds):
    with open('top_domain_10.csv', 'r') as f:
        top_domain_10 = f.read()
    with open('max_len_domain.txt', 'r') as f:
        max_len_domain = f.read()
    with open('domain_place.txt', 'r') as f:
        domain_place = f.read()    
    date = ds

    print(f'Top 10 domains for date {date}')
    print( top_domain_10)
    print(f'The longest domain name for date {date}')
    print( max_len_domain)
    print(f'The rank of airflow.com for date {date}')
    print(domain_place)


default_args = {
    'owner': 'n.eshmanskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=6),
    'start_date': datetime(2022, 12, 15),
}
schedule_interval = '10 11 * * *'

dag = DAG('n.eshmanskaja', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='max_len_domain',
                    python_callable=max_len_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_domain_rank',
                        python_callable=get_domain_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3,t4] >> t5