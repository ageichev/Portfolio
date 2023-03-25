import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


TOP10_DOMAIN_ZONES_FILE = 'top10-zones.txt'
LONGEST_DOMAIN_FILE = 'longest_domain.txt'
AIRFLOW_POSITION_FILE = 'airflow_position.txt'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def get_top10_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    zones = df['domain'].apply(lambda domain: domain.split('.')[-1])
    with open(TOP10_DOMAIN_ZONES_FILE, 'w') as f:
        f.write('\n'.join(zones.value_counts()[:10].index))
        
def get_longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_length = df['domain'].apply(len)
    max_value = domain_length.sort_values().iloc[-1]
    longest = df['domain'][domain_length.eq(max_value)].sort_values().iloc[0]
    with open(LONGEST_DOMAIN_FILE, 'w') as f:
        f.write(longest)
        
        
def get_airflow_pos():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = df[df['domain'].eq('airflow.com')]
    if airflow.empty:
        result = 'Домен "airflow.com" не найден'
    else:
        result = df['rank'].iloc[0]
    with open(AIRFLOW_POSITION_FILE, 'w') as f:
        f.write(str(result))
        
        
def print_data(ds):
    logs = []
    with open(TOP10_DOMAIN_ZONES_FILE, 'r') as f:
        logs.extend((f'{ds}: Топ-10 доменных зон по численности доменов:', f.read(), '\n'))
    with open(LONGEST_DOMAIN_FILE, 'r') as f:
        logs.extend((f'{ds}: Домен с самым длинным именем:', f.read(), '\n'))
    with open(AIRFLOW_POSITION_FILE, 'r') as f:
        logs.extend((f'{ds}: Позиция домена airflow.com:', f.read(), '\n'))
    print('\n'.join(logs))
        
        
default_args = {
    'owner': 's.mishina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 2),
}
schedule_interval = '0 8 * * *'

dag = DAG('s_mishina_w2_t4', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_zones',
                    python_callable=get_top10_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_pos',
                        python_callable=get_airflow_pos,
                        dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
