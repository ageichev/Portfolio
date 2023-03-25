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
        
def get_top_domein_areas():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    
    #creat column with domein zones
    top_data_df['domain_aria'] = top_data_df['domain'].str.split('.').str[-1]
    
    #count amount of domeins
    top_10_areas = (top_data_df.
                groupby('domain_aria', as_index = False).
                domain.count().
                sort_values(by = 'domain', ascending = False).
                rename(columns = {'domain': 'domain_num'}).
                head(10)
               )
    with open('top_10_areas.csv', 'w') as f:
        f.write(top_10_areas.to_csv(index=False, header=False))

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    #count lengh of domain

    top_data_df['lengh_domain'] = top_data_df.domain.map(lambda x : len(x))
    
    #choos the longest domains
    top_longest_domainS = top_data_df[top_data_df.lengh_domain == top_data_df.lengh_domain.max()]
    
    #alphabetical order and choose first domain
    top_longest_domain = top_longest_domainS.sort_values(by = "domain").head(1).domain
    
    with open('top_longest_domain.csv', 'w') as f:
        f.write(top_longest_domain.to_csv(index=False, header=False))
        
def get_index():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['order', 'domain'])

    #show required index
    try:
        required_index = f'Domein place is {top_data_df[top_data_df.domain == "airflow.com"].index[0]}'
    except: 
        required_index = 'Domain not exist'
    with open('required_index.txt', 'w') as f:
            f.write(required_index)
        
def print_data(ds):
    with open('top_10_areas.csv', 'r') as f:
        top_10 = f.read()
    with open('top_longest_domain.csv', 'r') as f:
        top_1 = f.read()
    with open('required_index.txt', 'r') as f:
        req_index = f.read()  
        
    date = ds

    print(f'Top domains numbers {date}')
    print(top_10)

    print(f'Longest domain {date}')
    print(f'Longest domein is {top_1}')
    
    print(f'"airflow.com" domein position {date}')
    print(req_index)
    
default_args = {
    'owner': 'm-rossihinapirozhkova-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 14),
}

schedule_interval = '0 10 * * *'

dag = DAG('m-rossihinapirozhkova_26_dag', default_args = default_args, schedule_interval=schedule_interval)

t0 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t1 = PythonOperator(task_id='get_top_domein_areas',
                    python_callable=get_top_domein_areas,
                    dag=dag)

t2 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_index',
                        python_callable=get_index,
                        dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t0 >> [t1, t2, t3] >> t4