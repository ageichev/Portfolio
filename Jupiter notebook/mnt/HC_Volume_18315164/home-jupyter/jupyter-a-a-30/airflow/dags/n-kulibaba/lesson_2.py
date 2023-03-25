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





# Таски
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_zone = top_data_df
    top_domain_zone['domain_zone'] = top_domain_zone.domain.str.split('.').str[-1]
    top_10_domain_zone = top_domain_zone.groupby('domain_zone', as_index=False).agg({'rank': 'count'}).sort_values('rank', ascending=False).head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))


def get_top_len_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_len_name = top_data_df
    top_len_name['len_domain'] = top_len_name.domain.apply(lambda x:len(x))
    top_len_name['domain_zone'] = top_len_name.domain.str.split('.').str[-1]
    top_len_name['len_domain_zone'] = top_len_name.domain_zone.apply(lambda x:len(x))
    top_len_name['len_name'] = top_len_name.len_domain - top_len_name.len_domain_zone 
    top_len_name = top_len_name.sort_values(['len_name', 'domain'], ascending = [False, True]).head(1)['domain']
    with open('top_len_name.csv', 'w') as f:
        f.write(top_len_name.to_csv(index=False, header=False))
        

def get_airflow_place():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_place = top_data_df.query("domain == 'airflow.com'")['rank']
    with open('airflow_place.csv', 'w') as f:
        f.write(airflow_place.to_csv(index=False, header=False))
        
        
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_domain_zone = f.read()
    with open('top_len_name.csv', 'r') as f:
        top_len_name = f.read()
    with open('airflow_place.csv', 'r') as f:
        airflow_place = f.read()        
    date = ds

    print('--------------------------------------------------------------------------------')
    print(f'top_10_domain_zone for date {date}')
    print(top_10_domain_zone)
    print('--------------------------------------------------------------------------------')
    print(f'long_len_name for date {date}')
    print(top_len_name)
    print('--------------------------------------------------------------------------------')
    print(f'airflow_place for date {date}')
    print(airflow_place)


    
    
# Инициализируем DAG
default_args = {
    'owner': 'n.kulibaba',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 16),
    'schedule_interval': '0 24 * * *'
}
dag = DAG('n-kulibaba', default_args=default_args)




# Инициализируем таски
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_top_len_name',
                        python_callable=get_top_len_name,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_airflow_place',
                        python_callable=get_airflow_place,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)



# Задаем порядок выполнения
t1 >> [t2_1, t2_2, t2_3] >> t3
