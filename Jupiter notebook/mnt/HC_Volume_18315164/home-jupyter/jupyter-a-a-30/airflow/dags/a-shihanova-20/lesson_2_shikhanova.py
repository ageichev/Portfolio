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

#Найти топ-10 доменных зон по численности доменов
def get_top_10_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain = top_data_df['domain_zone'].value_counts().reset_index().head(10)
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))


#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_name_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length_name'] = top_data_df['domain'].str.len()
    longest_name_domain = top_data_df.sort_values('domain') \
                        .sort_values('length_name', ascending=False) \
                        .head(1)['domain']
    with open('name_domain.csv', 'w') as f:
        f.write(name_domain.to_csv(index=False, header=False))


#На каком месте находится домен airflow.com?
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query("domain.str.contains('airflow.com')")['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False)) 
        
#Финальный таск должен писать в лог результат ответы на вопросы выше        
def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        top_10 = f.read()
    with open('name_domain.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow = f.read()
    date = ds

    print(f'Top 10 domain zones {date}:')
    print(top_10)

    print(f'Longest domain name {date}:')
    print(longest_name)

    print(f'Airflow.com rank {date}:')
    print(airflow)

    
    

default_args = {
    'owner': 'a.shikhanova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 14),
}
schedule_interval = '30 15 * * *'

dag_shikhanova = DAG('lesson_2_shikhanova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_shikhanova)

t2 = PythonOperator(task_id='get_top_10_domain',
                    python_callable=get_top_10_domain,
                    dag=dag_shikhanova)

t3 = PythonOperator(task_id='get_name_domain',
                        python_callable=get_name_domain,
                        dag=dag_shikhanova)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag_shikhanova)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_shikhanova)

t1 >> [t2, t3, t4] >> t5

