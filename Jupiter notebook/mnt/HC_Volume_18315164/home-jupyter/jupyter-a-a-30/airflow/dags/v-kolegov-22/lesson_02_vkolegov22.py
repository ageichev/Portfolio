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

def top_10_by_count():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_by_count = top_data_df['domain'].str.split('.').str[1].value_counts().reset_index()['index']
    top_10_by_count = top_10_by_count.head(10)
    with open('top_10_by_count.csv', 'w') as f:
        f.write(top_10_by_count.to_csv(index=False, header=False))
        
def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain = top_data_df.loc[top_data_df['domain'].str.len().sort_values(ascending=False).head(1).index.item()]
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.loc[top_data_df['domain'].str.endswith('airflow.com')].head(1)
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))        
        
def print_data(ds):
    with open('top_10_by_count.csv', 'r') as f:
        top_10_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain_data = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()    
    date = ds

    print(f'Top 10 domains by count for date {date}')
    print(top_10_data)

    print(f'The longest domain for date {date}')
    print(longest_domain_data)
    
    print(f'The airflow.com rank for date {date}')
    print(airflow_rank_data)


default_args = {
    'owner': 'v-kolegov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 25),
}
schedule_interval = '0 12 * * *'

dag = DAG('vkolegov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_by_count',
                    python_callable=top_10_by_count,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
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