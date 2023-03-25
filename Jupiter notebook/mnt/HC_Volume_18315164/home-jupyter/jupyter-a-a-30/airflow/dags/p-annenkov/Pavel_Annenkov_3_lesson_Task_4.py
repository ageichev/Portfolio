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
        
def get_top_10_domain_zones():
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_10_domain_zones = top_data_df
    top_10_domain_zones['domain_zones'] = top_data_df.domain.str.split('.').str[1]
    top_10_domain_zones['count'] = top_data_df.domain.str.split('.').str[1]
    
    top_10_domain_zones = top_10_domain_zones.groupby('domain_zones', as_index=False).agg({'count': 'count'}).\
        sort_values('count', ascending=False).reset_index(drop=True).head(10)
    
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))


def get_longest_domain():
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    list_of_domains = []
    domain_length = -1
    longest_domain_csv = pd.DataFrame()

    for i in range(top_data_df.shape[0]):
        if len(top_data_df.domain[i]) > domain_length:
            domain_length = len(top_data_df.domain[i]) 
    
    for i in range(top_data_df.shape[0]):
        if len(top_data_df.domain[i]) == domain_length:
            list_of_domains.append(top_data_df.domain[i])

    list_of_domains.sort()
    longest_domain = list_of_domains[0]
    longest_domain = pd.Series(longest_domain)
    longest_domain_csv = longest_domain_csv.append(longest_domain, ignore_index=True)
    longest_domain_csv = longest_domain_csv.rename(columns={0: 'longest_domain'})
    
    with open('longest_domain_csv.csv', 'w') as f:
        f.write(longest_domain_csv.to_csv(index=False, header=False))
        
def get_top_airflow():
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['place_in_the_top', 'domain'])
    
    top_airflow = top_data_df.query("domain == 'airflow.com'").place_in_the_top

    if not top_airflow.empty:
        top_airflow_csv = top_airflow

    else:
        top_airflow = 'airflow.com is not in top'
        top_airflow = pd.Series(top_airflow)
        top_airflow_csv = pd.DataFrame()
        top_airflow_csv = top_airflow_csv.append(top_airflow, ignore_index=True)
        top_airflow_csv = top_airflow_csv.rename(columns={0: 'airflow.com'})
    
    with open('top_airflow_csv.csv', 'w') as f:
        f.write(top_airflow_csv.to_csv(index=False, header=False))
        
def print_data(ds):
    
    with open('top_10_domain_zones.csv', 'r') as f:
        all_data_domain_zones = f.read()
    with open('longest_domain_csv.csv', 'r') as f:
        all_data_longest_domain = f.read()
    with open('top_airflow_csv.csv', 'r') as f:
        all_data_top_airflow = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(all_data_domain_zones)

    print(f'The longest_domain for date {date}')
    print(all_data_longest_domain)
    
    print(f'airflow.com top place for date {date}')
    print(all_data_top_airflow)

    

default_args = {
    'owner': 'p-annenkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 1),
}
schedule_interval = '0 12 * * *'

dag = DAG('p-annenkov_lesson_2_DAG', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_d = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag)

t2_l = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_a = PythonOperator(task_id='get_top_airflow',
                        python_callable=get_top_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_d, t2_l, t2_a] >> t3