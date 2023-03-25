import requests #
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
    top_data_top_10 = top_data_df.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_stat_longname():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    len_element,longest_element = max([(len(x),x) for x in (top_data_df['domain'])])
    with open('longest_element', 'w') as f:
        f.write(longest_element)

def get_stat_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_airflow = top_data_df[top_data_df['domain'] =='airflow.com'] #
    if top_airflow.isnull:
        with open('top_airflow', 'w') as f:
            f.write('Not found')
    else:
        with open('top_airflow', 'w') as f:
            f.write(top_airflow.to_csv(index=False, header=False))



def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('longest_element', 'r') as f:
        all_data_long = f.read()
    with open('top_airflow', 'r') as f:
        all_data_airflow = f.read()

    date = ds

    print(f'Top domains for date {date}')
    print(all_data)

    print(f'The lonest domains name for date {date}')
    print(all_data_long)

    print(f'Number of airflow.com for date {date}')
    print(all_data_airflow)

default_args = {
    'owner': 'a-kislitsin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 30),
}
schedule_interval = '0 12 * * *'

dag = DAG('a-kislitsin', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_stat_top_10',
                    python_callable=get_stat_top_10,
                    dag=dag)

t2_long = PythonOperator(task_id='get_stat_longname',
                        python_callable=get_stat_longname,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_stat_airflow',
                        python_callable=get_stat_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_long, t2_airflow] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
