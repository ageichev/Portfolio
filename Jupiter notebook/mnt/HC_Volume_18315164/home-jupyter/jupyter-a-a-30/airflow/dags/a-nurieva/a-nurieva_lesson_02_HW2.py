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


def get_top_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['ranks', 'domain'])
    top_data_df['domain']=top_data_df['domain'].astype(str)
    top_data_df['domain_zone'] = top_data_df.domain.apply(lambda x: x[x.rfind("."):])
    data_top_10 = top_data_df.groupby('domain_zone', as_index=False).agg({'domain':'count'})\
                    .sort_values('domain', ascending=False).head(10)
    with open('data_top_10.csv', 'w') as f:
        f.write(data_top_10.to_csv(index=False, header=False))
    
def get_domain_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['ranks', 'domain'])
    top_data_df['domain']=top_data_df['domain'].astype(str)
    top_data_df['domain_length'] = top_data_df.domain.apply(lambda x: len(x))
    max_length = top_data_df['domain_length'].max()
    max_domain = top_data_df.query(f'domain_length=={max_length}')\
            .sort_values('domain', ascending=True).head(1).domain.values[0]
    with open('max_domain.csv', 'w') as f:
        f.write(max_domain)

def get_airflow_info():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['ranks', 'domain'])
    top_data_df['domain']=top_data_df['domain'].astype(str)
    get_airflow_info = top_data_df.query('domain=="airflow.com"')
    
    with open('get_airflow_info.csv', 'w') as f:
        f.write(get_airflow_info.to_csv())   

        
def print_data(ds):
    with open('data_top_10.csv', 'r') as f:
        data_top_10 = f.read()
    with open('max_domain.csv', 'r') as f:
        max_domain = f.read()
    get_airflow_info = pd.read_csv('get_airflow_info.csv')      
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(data_top_10)
    
    print(f'The longest domain name for date {date} is {max_domain}')

    if len(get_airflow_info.ranks.values) == 0:
        print(f'There is no domain airflow.com for date {date}')
    else:
        print(f'For date {date} the rank of domain airflow.com:')
        for i in get_airflow_info.ranks.values:
            print(i, end = ' ')
    


default_args = {
    'owner': 'a-nurieva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 4),
}
schedule_interval = '0 13 * * *'

dag = DAG('domain_info_nurieva2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_top_domain_zone',
                    python_callable=get_top_domain_zone,
                    dag=dag)

t2_length = PythonOperator(task_id='get_domain_length',
                        python_callable=get_domain_length,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_info',
                        python_callable=get_airflow_info,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_length, t2_airflow] >> t3