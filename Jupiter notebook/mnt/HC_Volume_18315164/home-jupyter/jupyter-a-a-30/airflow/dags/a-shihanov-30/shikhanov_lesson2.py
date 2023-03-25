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
def get_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df["dom"]=top_data_df['domain'].str.split('.').str[-1]
    top_10_domain_zone = top_data_df.dom.value_counts().sort_values(ascending=False).reset_index().head(10)
    top_10_domain_zone.rename(columns = {'index' : 'domain', 'dom':'domain_zone'}, inplace = True)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))
def long_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    len_element,longest_element = max([(len(x),x) for x in (top_data_df['domain'])])
    with open('longest_element.csv', 'w') as f:
        f.write(longest_element)                
def airflow():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = data_df.query('domain=="airflow.com"')
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))       
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_element.csv', 'r') as f:
        longest = f.read()
    with open('airflow.csv', 'r') as f:
        air = f.read()
    date = ds

    print(f'----------------------------------------------------------------')
    print(f'Top 10 domain zones by number of domains for date {date}')
    print(top_10)
    print(f'----------------------------------------------------------------')
    print(f'Domain with the longest name for date {date}')
    print(longest)
    print(f'----------------------------------------------------------------')
    print(f'Where is the domain airflow.com {date}')
    print(air)
    print(f'----------------------------------------------------------------')

default_args = {
    'owner': 'a-shihanov-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 13),
    'schedule_interval': '0 10 * * *'
}
dag = DAG('a-shihanov-30', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_zone',
                    python_callable=get_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='long_name',
                        python_callable=long_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow',
                        python_callable=airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5





