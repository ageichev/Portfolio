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


def get_top_zone():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_zone_top_10 = top_doms['domain'].str.split(pat=".").str[1].value_counts()
    top_zone_top_10 = top_zone_top_10.head(10).reset_index()
    top_zone_top_10 = top_zone_top_10['index']
    with open('top_zone_top_10.csv', 'w') as f:
        f.write(top_zone_top_10.to_csv(index=False, header=False))


def get_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].map(lambda calc: len(calc))
    top_length_top_1 = top_data_df.sort_values(['length', 'domain'], ascending=[False, True]).head(1)
    with open('top_length_top_1.csv', 'w') as f:
        f.write(top_length_top_1.to_csv(index=False, header=False))


def get_airflow_rank():

    rank = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = rank.query('domain == "airflow.com"')
    if airflow_rank.shape[0] != 0:
        airflow_rank = airflow_rank.head(1)
    else:
        d = {'rank': ['out of top'], 'domain': ['airflow.com']}
        airflow_rank = pd.DataFrame(data=d)
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_zone_top_10.csv', 'r') as f:
        top_zone = f.read()
    with open('top_length_top_1.csv', 'r') as f:
        max_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow = f.read()

    date = ds

    print(f'Top zone for date {date}')
    print(top_zone)

    print(f'Max legth of domen for date {date}')
    print(max_length)

    print(f'Rank of airflow for date {date}')
    print(airflow)


default_args = {
    'owner': 's.dmitriev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=11),
    'start_date': datetime(2022, 10, 13),
}
schedule_interval = '0 23 * * *'

dag = DAG('s-dmitriev-25_neww', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_zone',
                    python_callable=get_top_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_length',
                    python_callable=get_length,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

# t1.set_downstream(t2)
# t1.set_downstream(t2_com)
# t2.set_downstream(t3)
# t2_com.set_downstream(t3)

