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
        
def get_stat_topzone():
    top_10_domain = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    top_10_domain['new'] = top_10_domain['domain'].str.split('.')
    top_10_domain['zone'] = top_10_domain['new'].apply(lambda x: x[-1])
    result_top10_domain = top_10_domain.groupby('zone',as_index=False).size().sort_values('size',ascending = False).head(10)
    with open('result_top10_domain.csv', 'w') as f:
        f.write(result_top10_domain.to_csv(index=False, header=False))

def get_largest_domain_name():
    top_10_domain = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    top_10_domain['new'] = top_10_domain['domain'].str.split('.')
    top_10_domain['name'] = top_10_domain['new'].apply(lambda x: x[0])
    top_10_domain['length'] = top_10_domain['name'].apply(lambda x: len(x))
    largest_domain = top_10_domain.sort_values(['length','name'], ascending=(False,True)).head(1)
    largest_domain = largest_domain[['name','length']]
    with open('largest_domain.csv', 'w') as f:
        f.write(largest_domain.to_csv(index=False, header=False))
        
def get_rank_airflow():
    top_10_domain = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    rank_airflow = top_10_domain.query('domain=="airflow.com"')[['rank','domain']]
    count_df_rows = rank_airflow.shape[0]
    print_frase = pd.Series('Airflow out of ranking')

    if count_df_rows == 0:
        with open('rank_airflow.csv', 'w') as f:
                f.write(print_frase.to_csv(index=False, header=False))
    else:
        with open('rank_airflow.csv', 'w') as f:
                f.write(rank_airflow.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('result_top10_domain.csv', 'r') as f:
        result_top10_domain = f.read()
    with open('largest_domain.csv', 'r') as f:
        largest_domain = f.read()
    with open('rank_airflow.csv', 'r') as f:
        rank_airflow = f.read()
    date = ds

    print(f'топ-10 доменных зон по численности доменов {date}')
    print(result_top10_domain)

    print(f'домен с самым длинным именем {date}')
    print(largest_domain)
    
    print(f'домен airflow.com находится в рейтинге на {date}')
    print(rank_airflow)

default_args = {
    'owner': 'k.agrova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 11),
    'schedule_interval': '0 10 * * *'
}
dag = DAG('k.agrova', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_topzone',
                    python_callable = get_stat_topzone,
                    dag=dag)

t2_com = PythonOperator(task_id='get_largest_domain_name',
                        python_callable= get_largest_domain_name,
                        dag=dag)
t2_rank = PythonOperator(task_id='get_rank_airflow',
                        python_callable= get_rank_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)
t1 >> [t2, t2_com, t2_rank] >> t3

