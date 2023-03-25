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

def get_data_pavel_baranov():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def top_10_domain_zones_pavel_baranov():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain_zones_1 = top_data_df['domain'].str.split('.').str[-1]
    top_10_domain_zones = top_10_domain_zones_1.value_counts().to_frame().head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))
        
def len_of_domain_pavel_baranov():
    len_of_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    len_of_domain_df['len_of_domain'] = len_of_domain_df.domain.apply(lambda x: len(x))
    len_of_domain_df.sort_values(['len_of_domain', 'domain'], ascending=[False, True]).head(1)
    with open('len_of_domain_df.csv', 'w') as f:
        f.write(len_of_domain_df.to_csv(index=False, header=False))
        
def airflow_rate_pavel_baranov():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rate = top_data_df.query('domain == "airflow.com"')
    with open('airflow_rate.csv', 'w') as f:
        f.write(airflow_rate.to_csv(index=False, header=False))
        
def print_data_pavel_baranov(ds): # передаем глобальную переменную airflow
    with open('top_10_domain_zones.csv', 'r') as f:
        all_data_zones = f.read()
    with open('len_of_domain_df.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow_rate.csv', 'r') as f:
        all_data_airflow_rate = f.read()
    date = ds

    print(f'Top domains zones for date {date}')
    print(all_data_zones)

    print(f'Domain with the longest name for date {date}')
    print(all_data_len)
    
    print(f'Rate of airflow.com in world rating for date {date}')
    print(all_data_airflow_rate)
    
default_args = {
    'owner': 'p.baranov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 18),
    'schedule_interval': '0 10 * * *'
}
dag = DAG('p.baranov', default_args=default_args)


t1 = PythonOperator(task_id='get_data_pavel_baranov',
                    python_callable=get_data_pavel_baranov,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zones_pavel_baranov',
                    python_callable=top_10_domain_zones_pavel_baranov,
                    dag=dag)

t3 = PythonOperator(task_id='len_of_domain_pavel_baranov',
                        python_callable=len_of_domain_pavel_baranov,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rate_pavel_baranov',
                        python_callable=airflow_rate_pavel_baranov,
                        dag=dag)

t5 = PythonOperator(task_id='print_data_pavel_baranov',
                    python_callable=print_data_pavel_baranov,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5