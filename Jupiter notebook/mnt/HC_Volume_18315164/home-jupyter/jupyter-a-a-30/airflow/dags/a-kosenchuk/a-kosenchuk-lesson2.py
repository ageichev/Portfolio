import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

data_domains = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
data_domains_file = 'top-1m.csv'

#Получение данных
def get_data():
    top_doms = requests.get(data_domains, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(data_domains_file).decode('utf-8')

    with open(data_domains_file, 'w') as f:
        f.write(top_data)


#ТОП-10 доменных зон по числу доменов
def top_10_domains():
    top_data_df = pd.read_csv(data_domains_file, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.str.split('.').str[-1]
    top_10_dom = top_data_df.groupby('zone', as_index=False).agg({'domain':'nunique'}).sort_values('domain', ascending=False).head(10).zone
    
    with open('data_top_10_dom.csv', 'w') as f:
        f.write(top_10_dom.to_csv(index=False, header=False))


#Домен с самым длинным именем
def largest_domain():
    top_data_df = pd.read_csv(data_domains_file, names=['rank', 'domain'])
    top_data_df['len_str'] = top_data_df.domain.str.len()
    larg_domain = top_data_df.sort_values('len_str', ascending=False).head(1).domain
    
    with open('data_larg_dom.csv', 'w') as f:
        f.write(larg_domain.to_csv(index=False, header=False))


#какой rank у домена airflow.com
def rank_airflow():
    top_data_df = pd.read_csv(data_domains_file, names=['rank', 'domain'])
    rank_aflow = top_data_df[top_data_df.domain == 'airflow.com']['rank']
    
    with open('rank_airfolw.csv', 'w') as f:
        f.write(rank_aflow.to_csv(index=False, header=False))


# передаем глобальную переменную airflow
def print_data(ds): 
    with open('data_top_10_dom.csv', 'r') as f:
        domain_zone = f.read()
    with open('data_larg_dom.csv', 'r') as f:
        dom_name = f.read()
    with open('rank_airfolw.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zone for date {date}')
    print(domain_zone )

    print(f'The largest domain name for date {date}')
    print(dom_name)
    
    print(f'Rank of airflow.com for date {date}')
    print(airflow_rank)


#Инициализируем DAG
default_args = {
    'owner': 'a-kosenchuk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 1),
    'schedule_interval': '10 /5 * * *'
}

dag = DAG('a-kosenchuk-hw2', default_args=default_args)


#Инициализируем таски
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domains',
                    python_callable=top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='largest_domain',
                        python_callable=largest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='rank_airflow',
                    python_callable=rank_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


#Задаем порядок выполнения

t1 >> [t2, t3, t4] >> t5