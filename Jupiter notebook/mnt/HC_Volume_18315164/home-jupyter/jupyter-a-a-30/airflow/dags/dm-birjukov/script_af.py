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
def top_10_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_10_domain_zone = top_data_df.groupby('domain_zone', as_index=False).agg({'rank': 'count'})\
        .sort_values('rank', ascending=False).head(10)

    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def longest_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].apply(len)
    longest_name = top_data_df[['domain', 'length']].sort_values('length', ascending=False)\
        .reset_index().loc[0]['domain']
       
    with open('longest_domain_name.csv', 'w') as f:
        print(longest_name, file=f)


# На каком месте находится домен airflow.com?
def airflowcom_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    with open('airflowcom_rank.csv', 'w') as f:
        if top_data_df[top_data_df['domain']=='airflow.com'].empty:
            f.write('airflow.com is not found')
        else:
            airflowcom_rank = top_data_df[top_data_df['domain']=='airflow.com']['rank']
            f.write(airflowcom_rank.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_zone.csv', 'r') as f:
        top_10_zone = f.read()
    with open('longest_domain_name.csv', 'r') as f:
        longest_domain_name = f.read()
    with open('airflowcom_rank.csv', 'r') as f:
        airflowcom_rank = f.read()
        
    date = ds

    print(f'Top ten zone to {date}')
    print(top_10_zone)

    print(f'Longest name of domain to {date}')
    print(longest_domain_name)

    print(f'Rank AIRFLOW domain to {date}')
    print(airflowcom_rank)

    
default_args = {
    'owner': 'dm-birjukov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 4),
    'schedule_interval': '0 */2 * * *'
}
dag = DAG('dag1_dm-birjukov', default_args=default_args)


t0 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t1 = PythonOperator(task_id='top_10_zone',
                    python_callable=top_10_zone,
                    dag=dag)

t2 = PythonOperator(task_id='longest_domain_name',
                    python_callable=longest_domain_name,
                    dag=dag)

t3 = PythonOperator(task_id='airflowcom_rank',
                        python_callable=airflowcom_rank,
                        dag=dag)

tend = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t0 >> [t1, t2, t3] >> tend

