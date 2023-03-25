import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    # Здесь запись в файл
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_domain_zone():
    # Топ-10 доменных зон
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_df['domain'] = top_domain_df['domain'].apply(lambda x: x.lstrip('www.'))
    top_domain_df['zone'] = top_domain_df.domain.str.split('.').str[0]
    top_domain_top_10 = top_domain_df.zone.value_counts().head(10).reset_index().rename(columns={'index':'zone', 'zone':'count'})
    with open('top_domain_top_10.csv', 'w') as f:
        f.write(top_domain_top_10.to_csv(index=False, header=False))

def get_max_len():
    # Найти домен с самым длинным именем
    domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_df['domain_len'] = domain_df.domain.str.len()
    domain_max_len = domain_df.sort_values('domain_len', ascending=False).head(1)
    with open('domain_max_len.csv', 'w') as f:
        f.write(domain_max_len.to_csv(index=False, header=False))

def get_airflow_place():
    # На каком месте находится airflow
    domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if 'airflow.com' in domain_df.domain.tolist():
        rank_airflow = domain_df.query("domain == 'airflow.com'")['rank'].values[0]
    else:
        rank_airflow = 'В списке отсутствует'
    with open('rank_airflow.txt', 'w') as f:
        f.write(rank_airflow)
    
        
def print_data(ds):
    with open('top_domain_top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('domain_max_len.csv', 'r') as f:
        max_len = f.read()
    with open('rank_airflow.txt', 'r') as f:
        rank_a = f.read()   
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10)

    print(f'The longest domain for date {date}')
    print(max_len)
    
    print(f'The airflow rank for date {date}')
    print(rank_a)


default_args = {
    'owner': 'o-safronova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 4),
}
schedule_interval = '15 21 * * *'

dag = DAG('o_safronova_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_zone',
                    python_callable=get_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len',
                        python_callable=get_max_len,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_place',
                        python_callable=get_airflow_place,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t3)
#t1.set_downstream(t4)
#t2.set_downstream(t5)
#t3.set_downstream(t5)
#t4.set_downstream(t5)
