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
def get_stat_top_10_domains_count():        
    domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])        
    top_10_domains_count = pd.DataFrame(
                                        domains.domain.str.rsplit(pat='.', n=1, expand=True).values, 
                                        columns=['name', 'domain'])\
                            .groupby('domain', as_index=False).agg({'name':'count'})\
                            .rename(columns={'name':'count'})\
                            .sort_values(by='count', ascending=False).head(10)        
    with open('top_10_domains_count.csv', 'w') as f:
        f.write(top_10_domains_count.to_csv(index=False, header=False))


#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)        
def get_stat_max_len_domain_name(): 
    domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len_domain_name = domains.query('domain.str.len() == domain.str.len().max()')\
                                    .sort_values(by='domain')\
                                    .head(1)
    with open('max_len_domain_name.csv', 'w') as f:
        f.write(max_len_domain_name.to_csv(index=False, header=False))         

        
#На каком месте находится домен airflow.com?
def get_stat_airflow_com_rank(): 
    domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_com_rank = domains.query('domain == "airflow.com"')[['rank']]
    with open('airflow_com_rank.csv', 'w') as f:
        f.write(airflow_com_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_domains_count.csv', 'r') as f:
        all_data_domains_count = f.read()
    with open('max_len_domain_name.csv', 'r') as f:
        all_data_domain_name = f.read()
    with open('airflow_com_rank.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top 10 domains count for date {date}')
    print(all_data_domains_count)

    print(f'Max len domain name for date {date}')
    print(all_data_domain_name)
    
    print(f'Airflow rank for date {date}')
    print(all_data_airflow)
        
default_args = {
    'owner': 'al-ermakov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 2),
}
schedule_interval = '0 12 * * *'

dag = DAG('al-ermakov_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_domains_count = PythonOperator(task_id='get_stat_top_10_domains_count',
                                    python_callable=get_stat_top_10_domains_count,
                                    dag=dag)

t2_max_len_domain_name = PythonOperator(task_id='get_stat_max_len_domain_name',
                                        python_callable=get_stat_max_len_domain_name,
                                        dag=dag)

t2_airflow_com_rank = PythonOperator(task_id='get_stat_airflow_com_rank',
                                        python_callable=get_stat_airflow_com_rank,
                                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_domains_count, t2_max_len_domain_name, t2_airflow_com_rank] >> t3