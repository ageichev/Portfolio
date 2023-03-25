import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_10_zones = df.zone.value_counts().head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(header=False))

        
def get_longest():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df.domain.str.len()
    longest_domain = df.sort_values(['length', 'domain'], ascending=[False, True]).head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False))

        
def get_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if 'airflow.com' not in df.domain.to_list():
        airflow_rank = 'airflow.com is not found'
    else:
        rank = df.query('domain == "airflow.com"')['rank'].values[0]
        airflow_rank = f'airflow.com is {rank} in rank'
    return airflow_rank


def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    date = ds
    airflow_rank = get_rank()

    print(f'Top 10 zones for date {date}')
    print(top_10_zones)

    print(f'Longest domain for date {date}')
    print(longest_domain)
    
    print(airflow_rank)


default_args = {
    'owner': 'a-furnik-27',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 20)
}


dag = DAG('a-furnik-27_lesson_02', default_args=default_args, schedule_interval='0 12 * * *')

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top',
                    python_callable=get_top,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank',
                        python_callable=get_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5