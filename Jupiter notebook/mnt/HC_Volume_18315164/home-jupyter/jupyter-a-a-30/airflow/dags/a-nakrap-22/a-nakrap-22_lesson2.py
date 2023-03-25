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


def get_top10_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_dom = top_data_df.domain.apply(lambda x: x.split('.')[-1]).value_counts().head(10)
    with open('top_10_dom.csv', 'w') as f:
        f.write(top_10_dom.to_csv(index=False, header=False))


def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.domain.apply(lambda x: len(x))
    longest_name_dom = top_data_df.sort_values(['domain_length', 'domain'], ascending=[False, True]).head(1)
    with open('longest_name_dom.csv', 'w') as f:
        f.write(longest_name_dom.to_csv(index=False, header=False))

        
def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    dom_airflow = top_data_df.loc[top_data_df.domain == 'airflow.com']
    try:
        rank = dom_airflow.iloc[0]['rank']
        result = f'Domain takes the {rank} place'
    except IndexError:
        result = 'Domain is not in the rating'
    with open('dom_airflow.csv', 'w') as f:
        f.write(result)

        
def print_data(ds):
    with open('top_10_dom.csv', 'r') as f:
        all_data = f.read()
    with open('longest_name_dom.csv', 'r') as f:
        all_data_len = f.read()
    with open('dom_airflow.csv', 'r') as f:
        all_data_air = f.read()    
    date = ds

    print(f'Top domain zones by number of domains for date {date}')
    print(all_data)

    print(f'Domain with the longest name for date {date}')
    print(all_data_len)
    
    print(f'Domain airflow.com for date {date}:')
    print(all_data_air)


default_args = {
    'owner': 'a.nakrap',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 30),
}
schedule_interval = '30 15 * * *'

dag = DAG('a_nakrap_lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_dom = PythonOperator(task_id='get_top10_dom',
                    python_callable=get_top10_dom,
                    dag=dag)

t2_long = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t2_air = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_dom, t2_long, t2_air] >> t3