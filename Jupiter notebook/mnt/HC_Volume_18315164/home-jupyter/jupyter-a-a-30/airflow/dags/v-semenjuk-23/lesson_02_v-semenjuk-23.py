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
        
def get_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    domain_zone_top_10 = top_data_df.domain.apply(lambda x: '.'.join(x.split('.')[1:])).value_counts().head(10)
    with open('domain_zone_top_10.csv', 'w') as f:
        f.write(domain_zone_top_10.to_csv(index=False, header=False))
                
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
        
def get_domain_max_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    domain_max_length = top_data_df.domain.apply(lambda x: len(x.split('.')[0])).max()
    domain_max_name = top_data_df['domain'][top_data_df.domain.apply(lambda x: len(x.split('.')[0])==domain_max_length)].sort_values().iloc[0]
    with open('domain_max_name.txt', 'w') as f:
        f.write(domain_max_name)

# На каком месте находится домен airflow.com?   

def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    rank_airflow = str(top_data_df['rank'][top_data_df.domain.str.contains('airflow')].iloc[0])
    with open('rank_airflow.txt', 'w') as f:
        f.write(rank_airflow)

        

def print_data(ds):
    with open('domain_zone_top_10.csv', 'r') as f:
        data_domain_zone = f.read()
    with open('domain_max_name.txt', 'r') as f:
        data_domain_max = f.read()
    with open('rank_airflow.txt', 'r') as f:
        data_rank_airflow = f.read()
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(data_domain_zone)

    print(f'Max name domain for date {date}')
    print(data_domain_max)
    
    print(f'Rank airflow for date {date}')
    print(data_rank_airflow)

    
    
default_args = {
    'owner': 'v-semenjuk-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 12),
}
schedule_interval = '0 8 * * *'

dag = DAG('v-semenjuk-23', default_args=default_args, 
schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_dzone = PythonOperator(task_id='get_domain_zone',
                    python_callable=get_domain_zone,
                    dag=dag)

t2_nmax = PythonOperator(task_id='get_domain_max_name',
                        python_callable=get_domain_max_name,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_dzone, t2_nmax, t2_rank] >> t3
