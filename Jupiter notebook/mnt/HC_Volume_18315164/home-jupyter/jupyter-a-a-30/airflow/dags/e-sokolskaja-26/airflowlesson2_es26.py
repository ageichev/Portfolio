#Импортируем библиотеки
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

#Подгружаем данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#Настраиваем таски
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top10zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone']=top_data_df['domain'].str.split('.').str[-1]    
    top10_domainzone = top_data_df.groupby("domain_zone", as_index=False).count().sort_values(by='domain', ascending=False)
    top10_domainzone.rename(columns = {'domain' : 'count'}, inplace = True) 
    top10_domainzone.head(10)
    with open('top10_domainzone.csv', 'w') as f:
        f.write(top10_domainzone.to_csv(index=False, header=False))


def get_max_length():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_length'] = df.domain.str.len()
    max_length_domain = df.sort_values(['domain_length', 'domain'], ascending=[False, True]).head(1)
    with open('max_length_domain.csv', 'w') as f:
        f.write(max_length_domain.to_csv(index=False, header=False))

        
def get_airflowrank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name'] = top_data_df['domain'].str.split('.').str[-2]
    airflow = top_data_df.query('domain_name=="airflow"')
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))


def print_data(ds): 
    with open('top10_domainzone.csv', 'r') as f:
        all_data = f.read()
    with open('max_length_domain.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow.csv', 'r') as f:
        all_data_name = f.read()
    date = ds

    print('Top 10  domain zone')
    print(all_data)

    print('The longest domain name')
    print(all_data_len)
       
    print('The rank of airflow.com')
    print(all_data_name)

#Инициализируем DAG
default_args = {
    'owner': 'e-sokolskaja-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 20),
    'schedule_interval': '0 10 * * *'
}
dag = DAG('e-sokolskaja-26_airflow_lesson2', default_args=default_args)

#Инициализируем таски
t1 = PythonOperator(task_id='get_data26',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10zone',
                    python_callable=get_top10zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length',
                        python_callable=get_max_length,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflowrank',
                        python_callable=get_airflowrank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data26',
                    python_callable=print_data,
                    dag=dag)

#Задаем порядок выполнения
t1 >> [t2, t3, t4] >> t5

#Другой способ задать порядок:
#t1.set_downstream(t2)
#t1.set_downstream(t3)
#t1.set_downstream(t4)
#t2.set_downstream(t5)
#t3.set_downstream(t5)
#t4.set_downstream(t5)

