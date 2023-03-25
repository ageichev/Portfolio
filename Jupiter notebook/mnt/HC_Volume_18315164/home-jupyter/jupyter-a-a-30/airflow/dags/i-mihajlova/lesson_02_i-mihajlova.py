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
        

def get_top_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_zones = df.zone.value_counts().head(10).to_frame().reset_index()
    with open('top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False, columns=['index']))

def longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df.domain.str.len()
    longest = df.sort_values(['length', 'domain'], ascending=[False, False]).head(1)
    with open('longest_domain.csv', 'w') as g:
        g.write(longest.to_csv(index=False, header=False, columns=['domain']))

def airflow_place():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if df.query("domain == 'airflow.com'").shape[0] == 0:
        res = 'airflow.com is not in top1M domains'
    else:
        res = str(df.query("domain == 'airflow.com'")['rank'][0])
    with open('airflow_place.csv', 'w') as h:
        h.write(res)
    
def print_data(ds):
    with open('top_zones.csv', 'r') as f:
        top_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest = f.read()
    with open('airflow_place.csv', 'r') as f:
        airflow_place = f.read()
    date = ds

    print(f'Top zones for date {date}')
    print(top_zones)

    print(f'Longest domain for date {date}')
    print(longest)
    
    print(f'Place of airflow.com for date {date}')
    print(airflow_place)


default_args = {
    'owner': 'i-mihajlova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 14),
}
schedule_interval = '0 * * * *'

dag = DAG('i_mihajlova_dag2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_place',
                        python_callable=airflow_place,
                        dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)