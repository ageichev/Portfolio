# Импортируем библиотеки

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Подгружаем данные

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# Таски

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# first function
def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name_domain'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_data_top_10 = top_data_df.groupby('name_domain', as_index=False).agg({'domain': 'count'}).\
                        rename(columns={'domain': 'amount'}).sort_values('amount', ascending=False).head(10)
    top_data_top_10_list = top_data_top_10['name_domain'].tolist()
    return top_data_top_10_list


# second function
def get_max_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['dom_len'] = top_data_df['domain'].str.len()
    index = top_data_df.sort_values(['dom_len', 'domain'], ascending=[False, True]).head(1).index
    longest_domain_name = top_data_df['domain'].values[index][0]
    return longest_domain_name

# third function
def get_row_number():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    index = top_data_df[top_data_df['domain'].str.contains('stackoverflow.com')].index
    row_number = top_data_df['rank'].values[index][0]
    return row_number


# Инициализируем DAG

default_args = {
    'owner': 'j.molchanova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 4),
    'schedule_interval': '35 22 * * *'
}
dag = DAG('j_molchanova_lesson2', default_args=default_args)

# Инициализируем таски

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length',
                        python_callable=get_max_length,
                        dag=dag)

t4 = PythonOperator(task_id='get_row_number',
                    python_callable=get_row_number,
                    dag=dag)


# Задаем порядок выполнения

t1 >> t2 >> t3 >> t4
