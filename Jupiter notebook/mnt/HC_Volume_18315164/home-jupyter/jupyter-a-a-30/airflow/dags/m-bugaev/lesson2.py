import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain=df.groupby('zone',as_index=False).size().sort_values(by='size',ascending=False,ignore_index=True).head(10)
    top_10_domain.columns=['zone','amount']
    top_10_domain
    with open('top_10.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))


def get_longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_length'] = df['domain'].str.len()
    domain_length = df.sort_values('domain_length', ascending=False).domain.head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(domain_length.to_csv(index=False, header=False))

def get_airflow_position():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_pos=df.loc[df.domain.str.contains('airflow.com')]['rank']
    with open('airflow_position.csv', 'w') as f:
        f.write(airflow_pos.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top10 = f.read()
    with open('longest_domain.csv', 'r') as f:
        long_domain = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_pos=f.read()
        
    date = ds

    print(f'Топ-10 доменных зон по численности на дату {date}:')
    print(top10)

    print(f'Домен с длинным именем на дату {date}: ')
    print(long_domain)
    
    print(f'Позиция airflow.com в общей ранке на дату {date}:')
    print(airflow_pos)

default_args = {
    'owner': 'm.bugaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 19),
}
schedule_interval = '0 12 * * *'

dag = DAG('m-bugaev', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t2_long = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_position',
                        python_callable=get_airflow_position,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_long, t2_airflow] >> t3