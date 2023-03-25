import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

domain_looking_for = 'airflow.com'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
        
def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_10_domain_zone = \
        top_data_df \
        .groupby('domain_zone') \
        .agg({'domain': 'count'}) \
        .rename(columns={'domain': 'cnt'}).sort_values(by='cnt', ascending=False) \
        .head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))

        
def get_the_longest_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].apply(lambda x: len(x))
    longest_name = top_data_df.sort_values(by=['domain_length', 'domain'], ascending=False)[:1]
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))
        
        
def looking_for_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    with open('domain_we_are_looking_for.csv', 'w') as f:
        if top_data_df[top_data_df['domain'] == domain_looking_for].empty:
            f.write('not found')
        else:
            f.write(df[df['domain'] == domain_looking_for]['rank'].values[0])


def print_data(ds):  # передаем глобальную переменную airflow
    with open('top_10_domain_zone.csv', 'r') as f:
        question_1 = f.read()
    with open('longest_name.csv', 'r') as f:
        question_2 = f.read()
    with open('domain_we_are_looking_for.csv', 'r') as f:
        question_3 = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(question_1)

    print(f'The_longest_domain_name for date {date}')
    print(question_2)
    
    print(f'Rank of {domain_looking_for} for date {date}')
    print(question_3)


default_args = {
    'owner': 'r.shokalo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 6),
}
schedule_interval = '0 18 * * *'

dag = DAG('rshokalo_lesson2_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_the_longest_domain_name',
                        python_callable=get_the_longest_domain_name,
                        dag=dag)

t2_looking = PythonOperator(task_id='looking_for_domain',
                        python_callable=looking_for_domain,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_longest, t2_looking] >> t3

#t1.set_downstream(t2_top)
#t1.set_downstream(t2_longest)
#t1.set_downstream(t2_looking)
#t2_top.set_downstream(t3)
#t2_longest.set_downstream(t3)
#t2_looking.set_downstream(t3)
