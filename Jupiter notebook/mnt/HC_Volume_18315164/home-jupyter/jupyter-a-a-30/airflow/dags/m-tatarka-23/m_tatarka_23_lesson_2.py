#%%
import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

#%%
from airflow import DAG
from airflow.operators.python import PythonOperator
#%%


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

#%%
def top_domain_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain_zone = df.domain.str.split(".").apply(lambda x: x[-1]).value_counts().head(10)
    with open("top_10_domain_zone.csv", "w") as f:
        f.write(top_10_domain_zone.to_csv(header=False))


def get_longest_domain_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df.domain.apply(lambda x: len(x))   
    result = df.sort_values(['length', 'domain'], ascending=[False,True]).iloc[0].domain    
    with open('longest_domain_name.txt', 'w') as f:
        f.write(result)

def get_airflow_rank():                                                                                         
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])                    
    if df[df.domain == 'airflow.com'].shape[0]:
        result = str(df[df.domain == 'airflow.com'].iloc[0]['rank'])
    else:
        result = 'airflow.com not in file'
    with open('airflow_rank.txt', 'w') as f:
        f.write(result)



def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_domain_zone = f.read()
    with open('longest_domain_name.txt', 'r') as f:
        longest_domain_name = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()

    date = ds

    print(f'Top domains zone for date {date}')
    print(top_10_domain_zone)

    print(f'Longest domain for date {date}')
    print(longest_domain_name)

    print(f'airflow rank for date {date}')
    print(airflow_rank)


#%%
default_args = {
    'owner': 'm.tatarka',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 20),
}
schedule_interval = '0 10 * * *'

#%%


#%%
dag = DAG('m_tatarka_23_lesson_2', default_args=default_args, schedule_interval=schedule_interval)



t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_domain_zone = PythonOperator(task_id='top_domain_zone',
                    python_callable=top_domain_zone,
                    dag=dag)

t2_longest_domain = PythonOperator(task_id='get_longest_domain_name',
                    python_callable=get_longest_domain_name,
                    dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_domain_zone, t2_longest_domain,t2_airflow] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
