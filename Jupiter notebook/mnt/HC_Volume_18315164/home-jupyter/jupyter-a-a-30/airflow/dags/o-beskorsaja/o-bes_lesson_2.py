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


def top_10_zone_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_10_zone_domain = df.groupby('zone').domain.count().sort_values(ascending=False).to_frame().head(10)
    with open('top_10_zone_domain.csv', 'w') as f:
        f.write(top_10_zone_domain.to_csv(header=False))
        
def longer_name_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longer_name_domain = df[df.domain.str.len() == max(df.domain.str.len())]['domain'].values[0]
    with open('longer_name_domain.txt', 'w') as f:
        f.write(longer_name_domain)
    
def place_airflow():
    try:
        df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        place_airflow = str(df.query("domain == 'airflow.com'").rank.values[0])
    except:
        place_airflow = '"airflow.com" there is no'
    with open('place_airflow.txt', 'w') as f:
        f.write(place_airflow)

def print_data(ds):
    with open('top_10_zone_domain.csv', 'r') as f:
        top_10_zone_domain = f.read()
    with open('longer_name_domain.txt', 'r') as f:
        longer_name_domain = f.read()
    with open('place_airflow.txt', 'r') as f:
        place_airflow = f.read()    
    date = ds

    print(f'Top domains zone by domain counts for date {date}')
    print(top_10_zone_domain)

    print(f'The longest domail name for date {date} is {longer_name_domain}')

    print(f'Airfow.com rank for date {date} is {place_airflow}')


default_args = {
    'owner': 'o-beskorsaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 8, 27),
}
schedule_interval = '@daily'

dag = DAG('o-bes_top_10_ru_new', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_zone_domain',
                    python_callable=top_10_zone_domain,
                    dag=dag)

t3 = PythonOperator(task_id='longer_name_domain',
                        python_callable=longer_name_domain,
                        dag=dag)
t4 = PythonOperator(task_id='place_airflow',
                        python_callable=place_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

