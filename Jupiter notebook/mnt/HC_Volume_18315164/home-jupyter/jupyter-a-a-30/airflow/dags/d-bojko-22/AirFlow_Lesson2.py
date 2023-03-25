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


def get_stat_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df['domain'].apply(lambda x: x.split(".")[-1]).value_counts().head(10).reset_index(). \
    rename(columns={'index': 'domain', 'domain': 'count'})
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_stat_top_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].apply(lambda x: len(x))
    top_length =  top_data_df.sort_values(by='length', ascending=False).head(1).domain
    with open('top_length.csv', 'w') as f:
        f.write(top_length.to_csv(index=False, header=False))

def get_stat_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if "airflow.com" in top_data_df.domain:
        airflow_rank = top_data_df.query('domain == "airflow.com"')['rank']
    else:
        airflow_rank = "airflow.com IS NOT present in dataset"
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data_top_10 = f.read()
    with open('top_length.csv', 'r') as f:
        all_data_top_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(all_data_top_10)

    print(f'The longest domain for date {date}')
    print(all_data_top_length)
    
    print(f'The rank for airflow.com for date {date}')
    print(all_data_airflow_rank)


default_args = {
    'owner': 'd-bojko-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 18),
}
schedule_interval = '30 8 * * *'

dag = DAG('my_first_awesome_dag_d_bojko_22', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_10 = PythonOperator(task_id='get_stat_top_10',
                    python_callable=get_stat_top_10,
                    dag=dag)

t2_top_length = PythonOperator(task_id='get_stat_top_length',
                        python_callable=get_stat_top_length,
                        dag=dag)

t2_airflow_rank = PythonOperator(task_id='get_stat_airflow_rank',
                        python_callable=get_stat_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_10, t2_top_length, t2_airflow_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

