import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO

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

        
def get_top_10_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.apply(lambda x: x.split('.')[-1:][0])
    top_10_domains = top_data_df.groupby('zone', as_index=False) \
                                .agg({'domain':'count'}) \
                                .sort_values(by='domain', ascending=False) \
                                .head(10) \
                                .reset_index() \
                                .drop(columns={'index', 'domain'})
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))

        
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.apply(lambda x: len('.'.join(str(x).split('.')[:-1])))
    longest_domain = top_data_df.sort_values(by='domain', ascending=False) \
                                .sort_values(by='length', ascending=False) \
                                .iloc[0,1]
    with open('longest_domain.txt', 'w') as f:
        f.write('Longest domain today is: {0}'.format(longest_domain))

        
def get_airflow_position():
    airflow_string = ''
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df[top_data_df.domain == 'airflow.com'].shape[0] != 0:
        with open('airflow_position.txt', 'w') as f:
            f.write('airflow.com domain is on {0} place!'.format(top_data_df[top_data_df.domain == 'airflow.com'].iloc[0,0]))
    else:
        with open('airflow_position.txt', 'w') as f:
            f.write('airflow.com domain is lower than {0} world domains today!'.format(top_data_df.shape[0]))

            
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domains.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
    with open('airflow_position.txt', 'r') as f:
        airflow_position = f.read()
    date = ds

    print(f'Top 10 domains to date {date}:')
    print(top_10)

    print(f'To date {date}:')
    print(longest_domain)
    
    print(f'To date {date}:')
    print(airflow_position)

default_args = {
    'owner': 'b_anisimov_20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 22),
}
schedule_interval = '30 17 * * *'

dag = DAG('b_anisimov_20', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                        python_callable=get_data,
                        dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                        python_callable=get_top_10_domain_zones,
                        dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_position',
                        python_callable=get_airflow_position,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                        python_callable=print_data,
                        dag=dag)

t1 >> [t2, t3, t4] >> t5 #555