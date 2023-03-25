#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_10_zone = top_data_df['zone'].value_counts().sort_values(ascending=False).head(10).reset_index().rename(columns={'index':'zone','zone':'count'})
    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10_zone.to_csv(index=False, header=False))


def get_max_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df['domain'].apply(lambda x: len(x.split('.')[0]))
    max_len = top_data_df.sort_values('len_domain', ascending=False)['domain_len'].max()
    max_len_domain = top_data_df.loc[top_data_df['len_domain'] == max_len]['domain'].iloc[0]
    with open('max_len_domain.txt', 'w') as f:
        f.write(max_len_domain)

        
def get_airflow_place():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airlow_place=top_data_df[top_data_df['domain']=='airflow.com']['rank'].iloc[0]
    if airlow_place>0:
        airlow_place=airlow_place
    else:
        airlow_place='airflow.com нет в списке'
    with open('airflow_place.txt', 'w') as f:
        f.write(airflow_place)    



def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_zone.csv', 'r') as f:
        top_10_zone = f.read()
    with open('max_len_domain.txt', 'r') as f:
        max_len_domain= f.read()
    with open('airflow_place.txt', 'r') as f:
        airflow_place= f.read()
    date = ds

    print(f'Top 10 zone for date {date}')
    print(top_10_zone)

    print(f'Max len dimain for date {date}')
    print(max_len_domain)
    
    print(f'Airflow place for date {date}')
    print(airflow_place)

default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 01, 30),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('a-panchenko', default_args=default_args)


# In[ ]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_top_zone',
                    python_callable=get_top_zone,
                    dag=dag)

t3_len = PythonOperator(task_id='get_max_len',
                        python_callable=get_max_len,
                        dag=dag)

t4_place= PythonOperator(task_id='get_airflow_place',
                        python_callable=get_airflow_place,
                        dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[ ]:


t1 >> [t2_top,t3_len,t4_place] >> t5

