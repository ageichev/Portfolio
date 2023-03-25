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

def top_domains():
    top_domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domains['domain'] = top_domains['domain'].apply(lambda x: x.split('.')[1])
    top_10_domains = top_domains.groupby('domain', as_index=False) \
                .agg({'rank': 'count'}) \
                .sort_values('rank', ascending=False) \
                .head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))
        
        
def longest_domain():
    longest_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    index = longest_domain.domain.str.len().sort_values(ascending=False).head(1).index
    longest_domain = longest_domain.loc[index]
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))
        
        
def airflow():
    airflow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = airflow.query('domain == "airflow.com"')
    if len(airflow) == 0:
        print('Такого домена нет!')
    else:
        with open('airflow.csv', 'w') as f:
            f.write(airflow.to_csv(index=False, header=False))
        
        
def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        all_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        all_data_long = f.read()
    with open('airflow.csv', 'r') as f:
        all_data_airflow = f.read()   
    
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)

    print(f'Longest domain for date {date}')
    print(all_data_long)
    
    print(f'Airflow position for date {date}')
    print(all_data_airflow)


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 23),
}
schedule_interval = '0 13 * * *'

dag = DAG('m-zhiganov-21', default_args=default_args, schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_domains',
                    python_callable=top_domains,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow',
                        python_callable=airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
