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


def show_top():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['region'] = top_data_df['domain'].str.split(pat = '.').str[1]
    top_data_top_10 = top_data_df.groupby('region').agg({'domain': 'count'}).sort_values(by = 'domain', ascending = False).head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain = top_data_df.loc[top_data_df['domain'].apply(len) == top_data_df['domain'].apply(len).max()].sort_values('domain', ascending=False).head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))       
        
        
def get_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank = top_data_df.loc[top_data_df['domain'] == 'airflow.com']['rank'].values
    if len(rank) != 0:
        final_rank = rank[0]
    else:
        print('Website is not found')
        final_rank = None
    res = pd.DataFrame([{"rank": final_rank}])    
    with open('rank.csv', 'w') as f:
        f.write(res.to_csv(index=False, header=False))  

        
def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('rank.csv', 'r') as f:
        rank = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top_10_data)

    print(f'The longest domain for date {date}')
    print(longest_domain)
    
    print(f'airflow.com rank for date {date}')
    print(rank)

default_args = {
    'owner': 'm.bukhtiyarova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 13),
}
schedule_interval = '* 12 * * *'

dag = DAG('Airflow_lesson_2_Bukhtiyayarova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='show_top',
                    python_callable=show_top,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank',
                    python_callable=get_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)