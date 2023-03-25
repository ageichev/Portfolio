import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime



from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').apply(lambda x:x[-1])
    top_10_by_amount = top_data_df\
        .groupby('zone', as_index = False)\
        .agg({'domain':'count'})\
        .sort_values(by = 'domain', ascending = False)\
        .rename(columns = {'domain':'amount_of_domain'})\
        .head(10)
    
    with open('top_10_by_amount.csv', 'w') as f:
        f.write(top_10_by_amount.to_csv(index=False))     


def get_longest_domain():
    domain_lenth = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_lenth['domain_name'] = domain_lenth['domain'].str.split('.').apply(lambda x:x[0])
    domain_lenth['lenth'] = domain_lenth['domain_name'].apply(lambda x:len(x))
    domain_lenth = domain_lenth.sort_values(by = ['lenth','domain_name'], ascending = [False,True]).head(1)
    with open('domain_lenth.csv', 'w') as f:
        f.write(domain_lenth.to_csv(index=False))
    
            
def get_airflow_place():
    domain_lenth = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_lenth['domain_name'] = domain_lenth['domain'].str.split('.').apply(lambda x:x[0])
    domain_lenth['lenth'] = domain_lenth['domain_name'].apply(lambda x:len(x))
    domain_lenth = domain_lenth.sort_values(by = ['lenth','domain_name'], ascending = [False,True])
    domain_lenth = domain_lenth.reset_index().reset_index()
    domain_lenth['level_0'] = domain_lenth['level_0'].apply(lambda x:x+1)
    domain_lenth['zone'] = domain_lenth['domain'].str.split('.').apply(lambda x:x[-1])
    airflow_place = domain_lenth.query('"airflow" in domain_name and "com" in zone')
    with open('airflow_place.csv', 'w') as f:
        f.write(airflow_place.to_csv(index=False, header=False))
        

def print_data(ds):
    
    with open('top_10_by_amount.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_domain_name.csv', 'r') as f:
        longest = f.read()
    with open('airflow_place.csv', 'r') as f:
        airflow = f.read()
        
    data = ds
    
    print(f'Топ-10 самых популярных по количеству доменов доменных зон на дату {data}:')
    print(top_10)
    
    print(f'Самое длинное доменное имя на дату {data}:')
    print(longest)

    print(f'Место airflow.com в рейтинге самых длинных доменных имен на дату {data}:')
    print(airflow)
    
default_args = {
    'owner': 's-djakonova-29',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 10),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('sdjakonova29_dag', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2 = PythonOperator(task_id='get_top_zone',
                    python_callable=get_top_zone,
                    dag=dag)
t3 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)
t4 = PythonOperator(task_id='get_airflow_place',
                    python_callable=get_airflow_place,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5