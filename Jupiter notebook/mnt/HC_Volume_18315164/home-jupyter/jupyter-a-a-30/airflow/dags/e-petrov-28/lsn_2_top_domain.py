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


# Найти топ-10 доменных зон по численности доменов
def top_10_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_zones = top_data_df.domain.str.extract(r'([^.]+$)').value_counts().head(10)
    with open('top_data_zones.csv', 'w') as f:
        f.write(top_data_zones.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def top_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.apply(len)
    longest_domain = top_data_df.sort_values(['len', 'domain'], ascending=[False, True]).domain.values[0]
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)
    

# На каком месте находится домен airflow.com
def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.apply(len)
    rank_airflow_com = top_data_df.sort_values(['len', 'domain'], ascending=[False, True]) \
                                  .reset_index(drop=True).query("domain == 'airflow.com'")
    if len(rank_airflow_com) == 0:
        rank_ = 'airflow.com not in the list'
    else:
        rank_ = rank_airflow_com.index[0]
    with open('rank_airflow_com.txt', 'w') as f:
        f.write(rank_)


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_zones.csv', 'r') as f:
        all_data = f.read()
    with open('longest_domain.txt', 'r') as f:
        long_domain = f.read()
    with open('rank_airflow_com.txt', 'r') as f:
        rank_airflow_com = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}: \n{all_data}')
    print(f'Top domain with longest name for date {date}: {long_domain}')
    print(f'Overall rank for airflow.com domain for date {date}: {rank_airflow_com}')


default_args = {
    'owner': 'e.petrov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2023, 1, 23),
    'schedule_interval': '30 15 * * *'
}
dag = DAG('e-petrov-28_domain_research', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='top_10_zones',
                      python_callable=top_10_zones,
                      dag=dag)

t2_2 = PythonOperator(task_id='top_domain',
                      python_callable=top_domain,
                      dag=dag)

t2_3 = PythonOperator(task_id='airflow_rank',
                      python_callable=airflow_rank,
                      dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3
