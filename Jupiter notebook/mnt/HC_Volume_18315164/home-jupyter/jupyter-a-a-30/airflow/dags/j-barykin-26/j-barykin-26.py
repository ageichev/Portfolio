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

def top_domain():
    top_10_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain['domain_final'] = top_10_domain['domain'].apply(lambda x: x.split('.')[1])
    top_10_domain = top_10_domain['domain_final'].value_counts().to_frame().reset_index().sort_values(by = 'domain_final', ascending = False).head(10)
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

def domain_len():
    domain_len = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_len['domain_len'] = domain_len['domain'].apply(lambda x: len(x))
    max_domain_len = domain_len.loc[domain_len['domain_len'].idxmax()].domain
    with open('max_domain_len.txt', 'w') as f:
        f.write(str(max_domain_len))

def airflow_rank():
    airflow_rank = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if 'airflow.com' in airflow_rank['domain']:
        result = int(airflow_rank.query('domain == "airflow.com"')['rank'])
    else:
        result = '> 10 M'
    with open('result.txt', 'w') as f:
        f.write(str(result))


def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        top_10_domain_zone = f.read()
    with open('max_domain_len.txt', 'r') as f:
        max_len_domain = f.read()
    with open('result.txt', 'r') as f:
        result = f.read()
    date = ds

    print(f'Top 10 domains by date {date}:')
    print(top_10_domain_zone)

    print(f'Maximum length domain:')
    print(max_len_domain)

    print('Rank of airwlof.com:')
    print(result)


default_args = {
    'owner': 'j-barykin-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=7),
    'start_date': datetime(2022, 11, 28),
    'schedule_interval': '30 14 * * *'
}

dag = DAG('j-barykin-26', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_domaine',
                    python_callable=top_domain,
                    dag=dag)

t3 = PythonOperator(task_id='domain_len',
                    python_callable=domain_len,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)