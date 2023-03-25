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

    #Найти топ-10 доменных зон по численности доменов
def get_biggest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_biggest_domain_zones = top_data_df.groupby('zone').agg({'rank' : 'count'}).sort_values('rank', ascending=False)
    top_biggest_domain_zones = top_biggest_domain_zones.head(10)
    with open('top_biggest_domain_zones.csv', 'w') as f:
        f.write(top_biggest_domain_zones.to_csv(index=False, header=False))

    #Найти домен с самым длинным именем 
def get_longest():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['len'] = data_df['domain'].str.len()
    the_longest_domain_name = data_df.sort_values('len', ascending = False).head(1)
    with open('the_longest_domain_name.csv', 'w') as f:
        f.write(the_longest_domain_name.to_csv(index=False, header=False))
    
    #На каком месте находится домен airflow.com?
def get_rank_airflow():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    the_airflow_rank = data_df.query('domain == "airflow.com"')
    with open('the_airflow_rank.csv', 'w') as f:
        f.write(the_airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_biggest_domain_zones.csv', 'r') as f:
        all_data_domains = f.read()
    with open('the_longest_domain_name.csv', 'r') as f:
        all_data_longest_name = f.read()
    with open('the_airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()
    date = ds

    print(f'Top biggest domain zones for date {date}')
    print(all_data_domains)

    print(f'The longest domain name for date {date}')
    print(all_data_longest_name)
    
    print(f'The airflow rank for date {date}')
    print(airflow_rank_data)


default_args = {
    'owner': 'e-ajbatova-31',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 1),
}
schedule_interval = '0 20 * * *'

dag = DAG('aybatova_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_biggest',
                    python_callable=get_biggest,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5