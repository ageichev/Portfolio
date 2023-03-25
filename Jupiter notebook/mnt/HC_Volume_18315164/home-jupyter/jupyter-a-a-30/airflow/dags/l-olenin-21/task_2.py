import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

URL_TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
FILE_NAME_TOP_1M_SITES = 'top-1m.csv'
FILE_NAME_TOP_10_DOMAINS = 'top_10_domains.csv'
FILE_NAME_LONGEST_DOMAIN = 'longest_domain.txt'
FILE_NAME_SITE_POSITION = 'site_position.txt'


def get_data():
    data = requests.get(URL_TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(data.content))
    df = zipfile.read(FILE_NAME_TOP_1M_SITES).decode('utf-8')

    with open(FILE_NAME_TOP_1M_SITES, 'w') as f:
        f.write(df)

# Топ-10 доменных зон по численности доменов
def get_top_10_domains(n=10):
    df = pd.read_csv(FILE_NAME_TOP_1M_SITES, names=['rank', 'domain'])
    domain_zone = df.domain.str.split('.').str[-1]
    top_domains = domain_zone.value_counts()[:n]

    with open(FILE_NAME_TOP_10_DOMAINS, 'w') as f:
        f.write(top_domains.to_csv(index=True, header=False))


# Домен с самым длинным именем
def get_longest_domain_name():
    df = pd.read_csv(FILE_NAME_TOP_1M_SITES, names=['rank', 'domain'])
    df['domain_name'] = df.domain.str.split('.').str[-2]
    df['length'] = df['domain_name'].apply(len)
    df.sort_values(['length', 'domain'], ascending=[False, True], inplace=True)

    with open(FILE_NAME_LONGEST_DOMAIN, 'w') as f:
        f.write(
            f'Longest domain name is "{df.iloc[0].domain}" with length {df.iloc[0].length}')


# Позиция домена
def get_position_of_domain(domain_name):
    df = pd.read_csv(FILE_NAME_TOP_1M_SITES, names=['rank', 'domain'])
    df_airflow = df.query('domain == @domain_name')

    if len(df_airflow) == 0:
        results = f'Domain "{domain_name}" is missing in top 1 million domains'
    else:
        results = f'Position of domain "{domain_name}" is {df_airflow.iloc[0]["rank"]}'

    with open(FILE_NAME_SITE_POSITION, 'w') as f:
        f.write(results)

# Вывод данных
def print_data(ds):

    print(f'For the state at {ds}')

    # Топ-10 доменных зон по численности доменов
    with open(FILE_NAME_TOP_10_DOMAINS, 'r') as f:
        print(f'Top 10 domain-zone:')
        print(f.read())

    # Домен с самым длинным именем
    with open(FILE_NAME_LONGEST_DOMAIN, 'r') as f:
        print(f.read())

    # Позиция домена airflow.com
    with open(FILE_NAME_SITE_POSITION, 'r') as f:
        print(f.read())

default_args = {
    'owner': 'l-olenin-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 1, 7, 45),
    'schedule_interval': '@daily'
}

dag = DAG('l-olenin-21_task_2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    op_kwargs={"n":10},
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain_name',
                    python_callable=get_longest_domain_name,
                    dag=dag)

t4 = PythonOperator(task_id='get_position_of_domain',
                    python_callable=get_position_of_domain,
                    op_kwargs={"domain_name": 'airflow.com'},
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5