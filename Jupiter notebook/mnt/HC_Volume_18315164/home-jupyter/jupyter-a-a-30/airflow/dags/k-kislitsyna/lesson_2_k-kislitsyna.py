import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

domains = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
domains_file = 'top-1m.csv'

def get_data():
    top_doms = pd.read_csv(domains)
    top_data = top_doms.to_csv(index=False)
    with open(domains_file, 'w') as f:
        f.write(top_data)

#топ 10 доменов
def get_top10_dom():
    domains_df = pd.read_csv(domains_file, names=['rank', 'domain_name'])
    top10_dom=domains_df.domain_name.str.split(".").str[-1].value_counts().head(10).reset_index()
    with open('top10_dom.csv', 'w') as f:
        f.write(top10_dom.to_csv(index=False, header=False, line_terminator='\n'))

#самый длинный домен
def get_maxlen_dom():
    domains_df = pd.read_csv(domains_file, names=['rank', 'domain_name'])
    domains_df['len']=domains_df['domain_name'].str.len()
    maxlen_df=domains_df.sort_values(['len','domain_name'],ascending=[False,True]).head(1)['domain_name']
    with open('maxlen_df.csv', 'w') as f:
        f.write(maxlen_df.to_csv(index=False, header=False))

#позиция airflow
def get_rank_airflow():
    domains_df = pd.read_csv(domains_file, names=['rank', 'domain_name'])
    rank_airflow=domains_df.query("domain_name=='airflow.com'")['rank']
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False))


def print_data(ds):
    with open('top10_dom.csv', 'r') as f:
        all_data_top10 = f.read()
    with open('maxlen_df.csv', 'r') as f:
        all_data_maxlen = f.read()
    with open('rank_airflow.csv', 'r') as f:
        all_data_rank_airflow = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data_top10)

    print(f'Max len domain for date {date}')
    print(all_data_maxlen)

    print(f'Rank airflow for date {date}')
    print(all_data_rank_airflow)

default_args = {
    'owner': 'k-kislitsyna',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 2),
}
schedule_interval = '30 13 * * *'

dag = DAG('k-kislitsyna_lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_dom = PythonOperator(task_id='get_top10_dom',
                    python_callable=get_top10_dom,
                    dag=dag)

t2_maxlen_dom = PythonOperator(task_id='get_maxlen_dom',
                        python_callable=get_maxlen_dom,
                        dag=dag)

t2_rank_airflow = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_dom, t2_maxlen_dom, t2_rank_airflow ] >> t3
