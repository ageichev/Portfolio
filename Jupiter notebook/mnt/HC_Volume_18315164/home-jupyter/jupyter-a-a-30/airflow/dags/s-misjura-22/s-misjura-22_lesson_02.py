import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Подгружаем данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_10():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data['domain_zone'] = df['domain'].str.split('.').str[-1]
    top_10=top_data.groupby('domain_zone', as_index=False).agg({'rank':'count'}).sort_values('rank',ascending=False).head(10)
    
    with open('top_domain_zone.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))

def long_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_long'] = df['domain'].apply(len)
    df[['domain','domain_long']].sort_values('domain_long',ascending=False).reset_index().loc[0]['domain']
    
    with open('long_name.csv', 'w') as f:
        f.write(long_name.to_csv(index=False, header=False))

def find_airflow():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        
    with open('find_airflow.csv', 'w') as f:
        if df[df ['domain']=='airflow.com'].empty:
            f.write('airflow.com is not found')
        else:
            find_airflow=df[df['domain']=='airflow.com']['rank']
            f.write(find_airflow)
        
        
def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('long_name.csv', 'r') as f:
        long_name = f.read()
    with open('find_airflow.csv', 'r') as f:
        find_airflow = f.read()
        
    date = ds

    print(f'Топ-10 доменных зон {date}:')
    print(top_10)
    print()

    print(f'Домен с самым длинным именем {date}:')
    print(long_name)
    print()

    print(f'Домен с именем {c} {date} находится на месте:')
    print(find_airflow)
    print()

# Инициализируем DAG
default_args = {
    'owner': 's-misjura-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 23),
}
schedule_interval = '00 9 * * *'

dag = DAG('s-misjura-22', default_args=default_args, schedule_interval=schedule_interval)

# Инициализируем таски
t1 = PythonOperator(task_id='get_data', python_callable=get_data, dag=dag)

t2 = PythonOperator(task_id='top_10', python_callable=top_10, dag=dag)

t3 = PythonOperator(task_id='long_name', python_callable=long_name,  dag=dag)

t4 = PythonOperator(task_id='find_airflow', python_callable=find_airflow, dag=dag)

t5 = PythonOperator(task_id='print_data',  python_callable=print_data,  dag=dag)

# Задаем порядок выполнения
t1 >> [t2, t3,t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)