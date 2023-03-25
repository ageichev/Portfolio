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

# Найти топ-10 доменных зон по численности доменов
def get_zones():
    top_data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data[['domain_name', 'tld']] = top_data['domain'].str.split('.', 1, expand=True)
    tld_counts = top_data['tld'].value_counts()
    top_10_tlds = tld_counts.head(10)
    top_10_tlds = top_10_tlds.reset_index().rename(columns = {'tld':'count'})
    with open('top_data_domain_zones_10.csv', 'w') as f:
        f.write(top_10_tlds.to_csv(index=False, header=False))

        
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_name():
    
    top_data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data[['domain_name', 'tld']] = top_data['domain'].str.split('.', 1, expand=True)
    top_data['domain_length'] = top_data['domain_name'].str.len()

    # Find the maximum length of the domain name
    max_length = top_data['domain_length'].max()

    # Filter the data frame to only include domains with the maximum length
    max_df = top_data[top_data['domain_length'] == max_length]

    # Sort the data frame in alphabetical order
    max_df = max_df.sort_values('domain_name')

    # Get the first domain in the sorted data frame
    max_df = max_df.iloc[0]
    
    with open('top_data_longest_domain_name.txt', 'w') as f:
        f.write(max_df.to_csv(index=False, header=False))

# На каком месте находится домен airflow.com?
def airflow_rank():
    top_data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        airflow_rank = str(top_data[top_data.domain == 'airflow.com']['rank'].values[0])
    except:
        airflow_rank = 'No info'
    with open('airflow_rank.txt', 'w') as f:
        f.write(airflow_rank)


def print_data(ds):
    with open('top_data_domain_zones_10.csv', 'r') as f:
        top_10_domain = f.read()
    with open('top_data_longest_domain_name.txt', 'r') as f:
        the_longest_domain_name = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()    
    date = ds

    print(f'Top domains zone by domain counts for date {date}')
    print(top_10_domain)
    
    print(f'#####################################################################')
    
    print(f'The longest domail name for date {date} is {the_longest_domain_name}')
    
    print(f'#####################################################################')
    print(f'Airfow.com rank for date {date} is {airflow_rank}')


# Инициализируем DAG
    
default_args = {
    'owner': 'a-velikzhanin-30',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 2, 13),
    'schedule_interval': '0 10 * * *'
}

dag = DAG('a-velikzhanin-30_l2', default_args=default_args)

# Инициализируем таски

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zones',
                    python_callable=get_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# Задаем порядок выполнения

t1 >> [t2, t3, t4] >> t5
