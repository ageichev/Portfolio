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
def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain'] = top_data_df['domain'].str.split('.').apply(lambda x: x[1])
    top_10_domain = top_data_df.groupby('domain',as_index=False).count().sort_values(by='rank',ascending=False)
    top_data_top_10 = top_10_domain.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))
def get_long():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_long = top_data_df.sort_values(by="domain", key=lambda x: x.str.len(), ascending=False)
    top = top_long.head(1)['domain'].iloc[0]
    with open('long_name', 'w') as f:
        f.write(top)
def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df[top_data_df['domain'] == 'airflow.com'].shape[0] != 0 :
        airflow_place = top_data_df[top_data_df['domain'] == 'airflow'].iloc[0]['rank']
    else:
        airflow_place = 'airflow.com not detected'
    with open('airflow_place.txt', 'w') as f:
        f.write(airflow_place)
def print_data(): 
    with open('top_data_top_10.csv', 'r') as f:
        top_domain = f.read()
    with open('long_name', 'r') as f:
        top_long_print = f.read()
    with open('airflow_place.txt', 'r') as f:
        airflow_place_print = f.read()
    print('top 10 in size doman')
    print(top_domain)
    print(' the longest name domain')
    print(top_long_print)
    print('place airflow')
    print(airflow_place_print)
default_args = {
    'owner': 'n-horkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 29),
}
schedule_interval = '0 14 * * *'
dag = DAG('n-horkov', default_args=default_args, schedule_interval=schedule_interval)
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)
t3 = PythonOperator(task_id='get_long',
                    python_callable=get_long,
                    dag=dag)
t4 = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag)
t5 = PythonOperator(task_id='print_data',
                    python_callable= print_data,
                    dag=dag)
t1 >> [t2, t2, t3, t4] >> t5

