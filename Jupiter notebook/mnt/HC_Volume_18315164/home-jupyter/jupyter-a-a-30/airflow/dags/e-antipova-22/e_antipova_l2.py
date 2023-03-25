import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)



def top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_top_10_domain_zone = top_data_df     .groupby('domain_zone', as_index=False)     .agg({'rank':'count'})     .sort_values('rank', ascending=False)    .head(10)
    
    
    with open('top_domain_zone.csv', 'w') as f:
        f.write(top_data_top_10_domain_zone.to_csv(index=False, header=False))


def long_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].apply(len)
    top_data_df[['domain', 'domain_length']].sort_values('domain_length', ascending=False).reset_index().loc[0]['domain']
    
    
    with open('long_domain_name.csv', 'w') as f:
        f.write(long_domain_name.to_csv(index=False, header=False))


def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
     
    
    with open('airflow_rank.csv', 'w') as f:
        if top_data_df[top_data_df['domain']=='airflow.com'].empty:
            f.write('airflow.com is not found')
        else:
            airflow_rank = top_data_df[top_data_df['domain']=='airflow.com']['rank']
            f.write(airflow_rank)




def print_data(ds): # передаем глобальную переменную airflow
    
    with open('top_domain_zone.csv', 'r') as f:
        top_data_top_10_domain_zone = f.read()
        
    with open('long_domain_name.csv', 'r') as f:
        long_domain_name = f.read()
        
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
        
    date = ds

    print(f'Top domain zone dor date {date}')
    print(top_data_top_10_domain_zone)

    print(f'longest_domain_name for date {date}')
    print(long_domain_name)
    
    print(f'position for airflow for date {date}')
    print(airflow_rank)



default_args = {
    'owner': 'e.antipova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 8),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('e_antipova', default_args=default_args, catchup=False)



t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zone',
                    python_callable=top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='long_domain_name',
                        python_callable=long_domain_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)



t1 >> [t2, t3, t4] >> t5





