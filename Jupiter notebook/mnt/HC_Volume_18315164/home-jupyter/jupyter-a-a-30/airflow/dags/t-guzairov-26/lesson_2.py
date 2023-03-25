import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке.
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10():
    top_10_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']).head(10)
    with open('top_10_df.csv', 'w') as f:
        f.write(top_10_df.to_csv(index=False, header=False))


def get_longest_name():
    longest_name = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_name = longest_name[longest_name.domain.str.len() == longest_name.domain.str.len().max()].sort_values(by='domain')
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))

        
def airflow_rank():
    af = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if len(af[af.domain.str.match('^airflow.com$')]) == 0:
        d = {'rank': ['not found'], 'domain': ['not found']}
        airflow_com_rank = pd.DataFrame(data=d)
    else:
        airflow_com_rank = af[af.domain.str.match('^airflow.com$')]
    with open('airflow.com_rank.csv', 'w') as f:
        f.write(airflow_com_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_df.csv', 'r') as f:
        top_10_df = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow.com_rank.csv', 'r') as f:
        airflowcom_rank = f.read()
    
    date = ds

    print(f'Top 10 domains for date: {date}')
    print(top_10_df)

    print(f'domain with longest name for date: {date}')
    print(longest_name)
    
    print(f'airflow.com rank for date: {date}')
    print(airflowcom_rank)


default_args = {
    'owner': 't.guzairov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 16),
}
schedule_interval = '30 10 * * *'

t_guzairov = DAG('top_domains', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=t_guzairov)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=t_guzairov)

t2_long = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=t_guzairov)

t2_af_rank = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=t_guzairov)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=t_guzairov)

t1 >> [t2, t2_long, t2_af_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_long)
#t1.set_downstream(t2_af_rank)
#t2.set_downstream(t3)
#t2_long.set_downstream(t3)
#t2_af_rank.set_downstream(t3)
