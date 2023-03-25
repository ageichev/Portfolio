import requests
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



def top_10_domain(): 
    
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    df_top_10_domain = df.groupby('zone').domain.count().sort_values(ascending=False).to_frame().head(10)
    with open('s_androsova_23_top_10_domain.csv', 'w') as f:
        f.write(df_top_10_domain.to_csv(header=False))



def longest_domain_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain_name = df[df.domain.str.len() == max(df.domain.str.len())]['domain'].values[0]
    with open('s_androsova_23_longest_domain_name.txt', 'w') as f:
        f.write(longest_domain_name)


def airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if df[df.domain == 'airflow.com'].shape[0]:
        result = df[df.domain == 'airflow.com'].iloc[0]['rank']
    else:
        result = 'No info'
    with open('s_androsova_23_airflow_rank.txt', 'w') as f:
        f.write(str(result))



def print_data(ds):
    with open('s_androsova_23_top_10_domain.csv', 'r') as f:
        top_10_domain = f.read()
    with open('s_androsova_23_longest_domain_name.txt', 'r') as f:
        longest_domain_name = f.read()
    with open('s_androsova_23_airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()    
    
    date = ds

    print(f'Top domains zone counts for date {date}')
    print(top_10_domain)

    print(f'The longest domain name for date {date} is {longest_domain_name}')
    
    print(f'Airfow.com rank for date {date} is {airflow_rank}')



default_args = {
    'owner': 's-androsova-23_lesson2',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 23),
}
schedule_interval = '@daily'

dag = DAG('lesson_02_s-androsova-23', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain',
                    python_callable=top_10_domain,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain_name',
                        python_callable=longest_domain_name,
                        dag=dag)
t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5