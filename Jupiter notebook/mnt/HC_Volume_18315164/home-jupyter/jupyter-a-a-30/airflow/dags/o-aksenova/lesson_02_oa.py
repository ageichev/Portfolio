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

# Скачать, распаковать архив, прочитать файл
def get_data_oa():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

#Найти топ-10 доменных зон по численности доменов
def get_domain_zones_oa():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'], index_col = 0)
    df['zone'] = df.domain.str.split('.').str[-1]
    top_10_zones = df.groupby('zone', as_index = False).domain.count().sort_values('domain', ascending = False).head(10)
    top_10_zones.rename(columns = {'domain': 'count'}, inplace = True)
    
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_max_dom_length_oa():
    df_2 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'], index_col = 0)
    df_2['length'] = df_2.domain.str.split('.').str[0]
    df_2['length'] = df_2['length'].apply(lambda x: len(x))
    max_domain_length = df_2.sort_values(['length', 'domain'], ascending = [False, True]).head(1)
    
    with open('max_domain_length.csv', 'w') as f:
        f.write(max_domain_length.to_csv(index=False, header=False))

# На каком месте находится домен airflow.com?
def airflow_rank_oa():
    df_3 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = df_3.loc[df_3.domain == "airflow.com"]
    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data_oa(ds): # передаем глобальную переменную airflow
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('max_domain_length.csv', 'r') as f:
        max_domain_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top-10 domain zones for date {date}')
    print(top_10_zones)

    print(f'Top-domain by max length for date {date}')
    print(max_domain_length)
    
    print(f'Airflow.com ranking for date {date} is "{airflow_rank}"')

default_args = {
    'owner': 'o.aksenova',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 1, 22),
    'schedule_interval': '0 17 * * *'
}
dag = DAG('lesson_2_airflow_oaksenova', default_args=default_args)

t1 = PythonOperator(task_id='get_data_oaks',
                    python_callable=get_data_oa,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_domain_zones_oaks',
                    python_callable=get_domain_zones_oa,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_max_dom_length_oaks',
                        python_callable=get_max_dom_length_oa,
                        dag=dag)

t2_3 = PythonOperator(task_id='airflow_rank_oaks',
                        python_callable=airflow_rank_oa,
                        dag=dag)

t3 = PythonOperator(task_id='print_data_oaks',
                    python_callable=print_data_oa,
                    dag=dag)


t1 >> [t2_1, t2_2, t2_3] >> t3