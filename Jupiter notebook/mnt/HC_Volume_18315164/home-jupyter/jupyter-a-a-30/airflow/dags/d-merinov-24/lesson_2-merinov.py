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


def get_stat_region():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['region'] = top_data_df.domain.str.split('.')
    list_comp = [top_data_df['region'][i][-1] for i in range(len(top_data_df['region']))]
    top_data_df['region'] = list_comp
    top_data_top_10 = top_data_df.groupby('region')['region'].count().sort_values(ascending=False).head(10)
    print (top_data_top_10)
    
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))

def get_top_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain = top_data_df.domain.to_frame().set_index(top_data_df.domain)\
                            .domain.str.len().sort_values().tail(1)
    with open('top_domain.csv', 'w') as f:
        f.write(top_domain.to_csv(index=False, header=False))


def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = top_data_df.query('domain == "airflow.com"')
    
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_domain.csv', 'r') as c:
        top_domain_data = c.read()
    date = ds

    print(f'Top regions for date {date}')
    print(all_data)

    print(f'The longest domain for date {date}')
    print(top_domain_data)
    
    try:
        with open('airflow.csv', 'r') as d:
            airflow_data = d.read():
            print(f'The airflow.com domain located for date {date}')
            print(airflow_data)
    except: 
        print(f'No airflow in list')

default_args = {
    'owner': 'd.merinov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 5, 10),
    'schedule_interval': '0 15 * * *'
}
dag = DAG('top_10_ru_d_merinov', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_region',
                    python_callable=get_stat_region,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_domain',
                        python_callable=get_top_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5