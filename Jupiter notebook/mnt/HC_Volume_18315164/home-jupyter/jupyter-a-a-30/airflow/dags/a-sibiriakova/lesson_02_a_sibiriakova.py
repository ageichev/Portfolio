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


def get_top_zones():
    # Найти топ-10 доменных зон по численности доменов
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_data_zones = top_data_df.groupby('zone', as_index = False) \
        .agg({'domain':'count'}) \
        .sort_values('domain', ascending = False) \
        .head(10)
    
    with open('top_data_zones.csv', 'w') as f:
        f.write(top_data_zones.to_csv(index=False, header=False))

        
def get_longest_name():
    # Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].apply(lambda x: len(x))
    top_data_longest_name = top_data_df.sort_values(['length', 'domain'], ascending = [False, True]).head(1)
    
    with open('top_data_longest_name.csv', 'w') as f:
        f.write(top_data_longest_name.to_csv(index=False, header=False))


def get_airflow_rank():
    # На каком месте находится домен airflow.com?
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_airflow_rank = top_data_df[top_data_df['domain'].str.contains('airflow.com')]   
    if top_data_airflow_rank.shape[0] == 0:
        top_data_airflow_rank = top_data_airflow_rank.append({'rank': 'no data', 'domain': 'airflow.com'}, ignore_index=True)
    
    with open('top_data_airflow_rank.csv', 'w') as f:
        f.write(top_data_airflow_rank.to_csv(index=False, header=False))        

        
def print_data(ds):
    with open('top_data_zones.csv', 'r') as f:
        zones = f.read()
    with open('top_data_longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('top_data_airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f"Top zones by amount of domains for date {date}")
    print(zones)

    print(f"The longest domain name for date {date}")
    print(longest_name)

    print(f"The airflow.com's rank for date {date}")
    print(airflow_rank)


default_args = {
    'owner': 'a.sibiriakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 20),
}
schedule_interval = '45 11 * * *'

dag = DAG('airflow_lesson_2_a_sibiriakova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3

#t1.set_downstream(t2_1)
#t1.set_downstream(t2_2)
#t1.set_downstream(t2_3)
#t2_1.set_downstream(t3)
#t2_2.set_downstream(t3)
#t2_3.set_downstream(t3)