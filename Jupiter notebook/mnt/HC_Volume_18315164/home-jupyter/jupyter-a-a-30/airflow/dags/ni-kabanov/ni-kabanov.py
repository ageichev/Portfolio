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

        
def topzone_10():
    df = pd.read_csv('top-1m.csv', names=['rank', 'domain'])
    df['zone'] = df.domain.apply(lambda x: x[x.index('.'):])
    topzone = df.value_counts('zone').head(10).reset_index().rename(columns={0:'count'})
    
    with open('topzone.csv', 'w') as f:
        f.write(topzone.to_csv(index=False, header=False))

    
def longest_name():
    df = pd.read_csv('top-1m.csv', names=['rank', 'domain'])
    df['name'] = df.domain.apply(lambda x: x[:x.index('.')])
    df['name_length'] = df.domain.apply(lambda x: len(x))
    
    longest_name = df[['domain', 'name_length']].sort_values('name_length', ascending=False)
    longest_name = df[df.name_length == int(longest_name.name_length[:1])]
    longest_name = longest_name.sort_values('name_length').head(1).domain.to_list()[0]
    
    with open('longest_name.txt', 'w') as f:
        f.write(longest_name)


def airflow_rank():
    df = pd.read_csv('top-1m.csv', names=['rank', 'domain'])
    rank = ''
    if 'airflow.com' in df.domain:
        rank = "airflow.com's ranks is {}".format(int(df[df.domain == "airflow.com"]['rank']))
    else:
        rank = 'airflow.com is absent'

    with open('rank.txt', 'w') as f:
        f.write(rank)
        
        
        
def print_result(ds):
    with open('topzone.csv', 'r') as f:
        topzone = f.read()
    with open('longest_name.txt', 'r') as f:
        longest_name = f.read()
    with open('rank.txt', 'r') as f:
        rank = f.read()
        
    date = ds

    print(f"Top 10 domain's zones for date {date}")
    print(topzone)
    
    print(f"Longest domain name for date {date}")
    print(longest_name)
    print()
    print(f'Date {date}.',rank)

    
default_args = {
    'owner': 'ni-kabanov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 24),
}
schedule_interval = '59 23 * * *'

dag = DAG('ni-kabanov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_0 = PythonOperator(task_id='topzone_10',
                    python_callable=topzone_10,
                    dag=dag)

t2_1 = PythonOperator(task_id='longest_name',
                        python_callable=longest_name,
                        dag=dag)

t2_2 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_result',
                    python_callable=print_result,
                    dag=dag)

t1 >> [t2_0, t2_1, t2_2] >> t3
