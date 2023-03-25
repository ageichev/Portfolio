#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


# In[2]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[55]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top10_domains():   
    top_domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domains["top_domain"] = top_domains.domain.str.split(".").str[-1]
    top_domains = top_domains.top_domain.value_counts().sort_values(ascending=False).reset_index().head(10)
    top_domains.rename(columns={"index":"top_domain","top_domain":"count"},inplace = True)
    with open("top10_domains.csv", "w") as f:
        f.write(top_domains.to_csv(index=False, header=False))


def get_long_name():
    long_name = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    long_name['LENGTH']=long_name['domain'].astype(str).str.len()
    long_name =long_name.sort_values("LENGTH",ascending=False).domain.head(1).astype(str).iloc[0]
    with open('the_longest.csv', 'w') as f:
        f.write(long_name) 
    
    
def airflow():   
    airflow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = airflow[airflow['domain'] =='airflow.com']["rank"].iat[0].astype(str)
    with open("top_airflow.csv", "w") as f:
        f.write(airflow)          
        
        
def print_data(ds): # передаем глобальную переменную airflow
    with open('top10_domains.csv', 'r') as f:
        top_domains = f.read()
    with open('the_longest.csv', 'r') as f:
        long_name = f.read()
    with open('top_airflow.csv', 'r') as f:
        airflow = f.read()
    date = ds
    
    print(f'Top domains zone by domain counts for date {date} is')
    print(top_domains)
    
    print(f'The logest domain for date {date} is')
    print(long_name)

    print(f'Airfow rank for date {date} in')
    print(airflow)


# In[4]:


default_args = {
    'owner': 'g-seleznev-31',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 2),
    'schedule_interval': '0 11 * * *'
}
dag = DAG('top_10_ru_new', default_args=default_args)


# In[5]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top10_domains',
                    python_callable=top10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_name',
                        python_callable=get_long_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow',
                    python_callable=airflow,
                    dag=dag)
t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[6]:


t1 >> [t2,t3,t4] >> t5

