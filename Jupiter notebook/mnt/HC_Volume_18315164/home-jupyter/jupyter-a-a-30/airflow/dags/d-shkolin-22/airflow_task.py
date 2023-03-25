import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'
current_time=datetime.now()

def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_10():
    data=pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank','domain'])
    data['extension']=data['domain'].apply(lambda x:x[x.rfind('.')+1:])
    top_10_extensions=data.groupby(['extension'],as_index=False).count()[['extension','rank']].sort_values(by='rank',ascending=False).head(10)
    with open ('top_10_extensions.csv','w') as f:
        f.write(top_10_extensions.to_csv(index=False, header=False))

def longest_domain():
    data=pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank','domain'])
    data['extension']=data['domain'].apply(lambda x:x[x.rfind('.')+1:])
    data['extension_length']=data['extension'].apply(lambda x:len(x))
    data=data.sort_values(by=['extension_length','extension'],ascending=[False,True])
    longest_domain=list(data.head(1)['extension'])[0]
    with open('longest_domain.txt','w') as f:
        f.write(longest_domain)

def airflow_domain():
    data=pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank','domain'])
    data['extension']=data['domain'].apply(lambda x:x[x.rfind('.')+1:])
    data['extension_length']=data['extension'].apply(lambda x:len(x))
    data=data.sort_values(by=['extension_length','extension'],ascending=[False,True])
    try:
        airflow_domain=int(str(data[data['domain']=='airflow.com']['rank']))
    except ValueError:
        airflow_domain='domain was not found in the dataset'
    with open('airflow_domain.txt','w') as f:
        f.write(airflow_domain)

def print_data(ds):
    with open('top_10_extensions.csv','r') as f:
        top_10_dom=f.read()
    with open('longest_domain.txt','r') as f:
        longest_domain=f.read()
    with open('airflow_domain.txt','r') as f:
        airflow_domain=f.read()
    date = ds

    print(f'Top 10 domains for date {current_time}')
    print(top_10_dom)

    print(f'Longest domain for date {current_time}')
    print(longest_domain)

    print(f'Airflow rank is for date {current_time}')
    print(airflow_domain)

default_args = {
    'owner': 'd.shkolin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'start_date': datetime(2022, 10, 16),
}
schedule_interval = '0 6 * * *'

dag = DAG('d-shkolin', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10',
                    python_callable=top_10,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                    python_callable=longest_domain,
                    dag=dag)
t4 = PythonOperator(task_id='airflow_domain',
                    python_callable=airflow_domain,
                    dag=dag)
t5 = PythonOperator(task_id='print_data',
                    python_callable=airflow_domain,
                    dag=dag) 

t1 >> t2>> t3>> t4>>t5
