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


def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zones'] = top_data_df['domain'].apply(lambda x:x.split('.')[-1])
    top_data_top_10 = top_data_df['zones'].value_counts().head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name_length'] = top_data_df['domain'].apply(lambda x:len(x))
    longest_name = top_data_df[['domain', 'name_length']].sort_values(by='name_length', ascending=False).head(1)
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))
        

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain.str.contains("airflow.com")')[['rank', 'domain']]    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10_zones = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 domains by number for date {date}')
    print(top_10_zones)

    print(f'The longest domain name for date {date}')
    print(longest_name)
    
    print(f'The rank of airlow.com for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'g.manzhieva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 19),
}
schedule_interval = '30 8 * * *'

dag = DAG('g-mandzhieva_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t2_long_name = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top10, t2_long_name, t2_rank] >> t3

#t1.set_downstream(t2_top10)
#t1.set_downstream(t2_long_name)
#t1.set_downstream(t2_rank)
#t2_top10.set_downstream(t3)
#t2_long_name.set_downstream(t3)
#t2_rank.set_downstream(t3)
