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

        
def get_top_10_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    df_top_10_domain = df.groupby('zone').domain.count().sort_values(ascending=False).to_frame().head(10)
    with open('df_top_10_domain.csv', 'w') as f:
        f.write(df_top_10_domain.to_csv(header=False))
        
def the_longest_domain_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    the_longest_domain_name = df[df.domain.str.len() == max(df.domain.str.len())]['domain'].values[0]
    with open('the_longest_domain_name.txt', 'w') as f:
        f.write(the_longest_domain_name)
    
def airflow_rank():
    try:
        df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        airflow_rank = str(df[df.domain == 'airflow.com']['rank'].values[0])
    except:
        airflow_rank = 'No info'
    with open('airflow_rank.txt', 'w') as f:
        f.write(airflow_rank)
    
        
        


def print_data(ds):
    with open('df_top_10_domain.csv', 'r') as f:
        top_10_domain = f.read()
    with open('the_longest_domain_name.txt', 'r') as f:
        the_longest_domain_name = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()    
    date = ds

    print(f'Top domains zone by domain counts for date {date}')
    print(top_10_domain)

    print(f'The longest domail name for date {date} is {the_longest_domain_name}')
    print(f'Airfow.com rank for date {date} is {airflow_rank}')


default_args = {
    'owner': 'a-arseneva-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 8, 18),
}
schedule_interval = '@daily'

dag = DAG('top_10_ru_arseneva_aa', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain',
                    python_callable=get_top_10_domain,
                    dag=dag)

t3 = PythonOperator(task_id='the_longest_domain_name',
                        python_callable=the_longest_domain_name,
                        dag=dag)
t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)