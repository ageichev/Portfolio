import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def zone10_domain(): # топ-10 доменных зон по численности доменов
    zone10 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    zone10['domain_zone'] = zone10['domain'].apply(lambda x: x.split('.')[-1])
    zone10_is = zone10.groupby('domain_zone', as_index=False).agg({'domain':'nunique'}).sort_values('domain',         ascending=False).head(10)
    with open('zone10_is.csv', 'w') as f:
        f.write(zone10_is.to_csv(index=False, header=False))
        
def longest_domain(): # домен с самым длинным именем
    longest = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest['name_len'] = longest['domain'].apply(lambda x: len(x))
    longest_is = longest.nlargest(1,'name_len')['domain']
    with open('longest_is.csv', 'w') as f:
        f.write(longest_is.to_csv(index=False, header=False))
        
def airflow(): # На каком месте находится домен airflow.com?
    airflow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow['len_domain']=airflow['domain'].apply(lambda x: len(x))
    airflow['rank_max_name']=airflow['len_domain'].rank(method='first', ascending=False)
    airflow_is=airflow.query('domain=="airflow.com"')['rank_max_name']
    with open('airflow_is.csv', 'w') as f:
        f.write(airflow_is.to_csv(index=False, header=False))


def print_data(ds):
    with open('zone10_is.csv', 'r') as f:
        all_data = f.read()
    with open('longest_is.csv', 'r') as f:
        all_data_com = f.read()
    with open('airflow_is.csv', 'r') as f:
        all_data_com = f.read()
    date = ds

    print(f'Топ 10 доменных зон {date}')
    print(zone10_is)

    print(f'Домен с самым длинным именем {date}')
    print(longest_is)
              
    print(f'На каком месте airflow.com {date}')
    print(airflow_is)



default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
}
schedule_interval = '0 12 * * *'

dag = DAG('d-bikbaeva-21_domain', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='zone10_domain',
                    python_callable=zone10_domain,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow',
                    python_callable=airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2,t3,t4] >> t5



