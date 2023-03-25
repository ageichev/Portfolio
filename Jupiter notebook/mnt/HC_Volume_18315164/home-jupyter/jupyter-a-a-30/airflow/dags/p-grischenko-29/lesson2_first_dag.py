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


def get_top_ten():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_data_df['zone'] = top_data_df['domain'].str.split('.').apply(lambda x:x[-1])
    res_top_10 = top_data_df \
                            .groupby('zone', as_index=False) \
                            .agg({'domain' : 'count'}) \
                            .rename(columns={'domain' : 'num_of_zones'}) \
                            .sort_values('num_of_zones', ascending=False) \
                            .head(10)
    
    with open('res_top_10.csv', 'w') as f:
        f.write(res_top_10.to_csv(index=False, header=False))


def get_biggest_len():
    length = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    length['domain_name'] = length['domain'].str.split('.').apply(lambda x:x[0])
    length['len'] = length['domain_name'].apply(lambda x:len(x))
    length = length.sort_values(by = ['len','domain_name'], ascending = [False,True]).head(1)
    biggest_length = length['domain_name']
    
    with open('biggest_length.csv', 'w') as f:
        f.write(biggest_length.to_csv(index=False, header=False))
        
        
        
def airflow_num():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    with open('airflow_num.csv', 'w') as f:
        airflow_num = df[df['domain'] == 'airflow.com']['rank']
        airflow_num = str(airflow_num)
        airflow_num = airflow_num.split(' ')[0]
        f.write(airflow_num)
    


def print_data(ds): # передаем глобальную переменную airflow
    with open('res_top_10.csv', 'r') as f:
        res_top_10_data = f.read()
    with open('biggest_length.csv', 'r') as f:
        biggest_length_data = f.read()
    with open('airflow_num.csv', 'r') as f:
        airflow_num = f.read()
    date = ds

    print(f'Топ-10 доменных зон по численности доменов: {date}')
    print(res_top_10_data)

    print(f'Домен с самым длинным именем: {date}')
    print(biggest_length_data)
    
    print(f'Место домена "airflow.com" в топе: {date}')
    print(airflow_num)


default_args = {
    'owner': 'p.grischenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 9),
    'schedule_interval': '0 22 * * *'
}
dag = DAG('latest_version_first_dag', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_ten',
                    python_callable=get_top_ten,
                    dag=dag)

t3 = PythonOperator(task_id='get_biggest_len',
                        python_callable=get_biggest_len,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_num',
                        python_callable=airflow_num,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5