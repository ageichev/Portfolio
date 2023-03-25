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
        

def get_top10_dz():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['dz'] = df.domain.apply(lambda x: '.'.join(x.split('.')[1:]))
    top_10_domain_zone = df.dz.value_counts().to_frame().reset_index().head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))     
     
    
def get_longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['len'] = df.domain.apply(lambda x: len(x.split('.')[0]))
    longest_domain = df[['domain', 'len']].sort_values(['len', 'domain'], ascending=[False, True]).head(1).domain.values[0]
    with open('longest_domain', 'w') as f:
        f.write(longest_domain)
        
        
def get_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if df.query("domain == 'airflow.com'").empty:
        rank = "The domain 'airflow.com' is out of the list"
    else:
        rank = str(df.query("domain == 'airflow.com'")['rank'].values[0])
    with open('rank', 'w') as f:
        f.write(rank)

        
def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_dz = f.read()
    with open('longest_domain', 'r') as f:
        longest_domain = f.read()
    with open('rank', 'r') as f:
        rank = f.read()
    
    date = ds

    print(f'Top 10 domain zones for date {date}:')
    print(top_10_dz)

    print(f'Longest domain for date {date}:')
    print(longest_domain)
    
    print(f'Rank of "airflow.com" for date {date}:')
    print(rank)


default_args = {
    'owner': 'k.konoplev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 20),
}
schedule_interval = '0 15 * * *'

dag = DAG('k-konoplev-26', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_dz',
                    python_callable=get_top10_dz,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank',
                        python_callable=get_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)