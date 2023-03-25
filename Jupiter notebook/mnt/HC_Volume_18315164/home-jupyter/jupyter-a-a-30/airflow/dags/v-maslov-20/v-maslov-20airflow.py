#!/usr/bin/env python
# coding: utf-8

# In[25]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[26]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[27]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top10_doms():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top10doms = df.domain.str.split('.').str[1].value_counts().reset_index().head(10)['index']
    with open('top10doms.csv', 'w') as f:
        f.write(top10doms.to_csv(index=False, header=False))
        
        
        
def maxlendom():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    maxlen = df.domain.str.len()
    argmax = np.where(maxlen == maxlen.max())[0]
    maxlendom = str(list(df.iloc[argmax].domain))
    with open('maxlendom.csv', 'w') as f:
        f.write(maxlendom)

def airflowplace():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    aifwp = df.query("domain == 'airflowacademy.com'")['rank']
    with open('aifwp.csv', 'w') as f:
        f.write(aifwp.to_csv(index=False, header=False))


def print_data(ds): #   передаемглобальную переменную airflow
    with open('top10doms.csv', 'r') as f:
        topdoms = f.read()
    with open('maxlendom.csv', 'r') as f:
        maxlen = f.read()
    with open('aifwp.csv', 'r') as f:
        aifwp = f.read()
    date = ds

    print(f'Top domains zone for date {date}')
    print(topdoms)

    print(f'Domain name with max len for date {date}')
    print(maxlen)
    
    print(f'\nPlace of airflowacademy.com domain for date {date}')
    print('No airflow.com. No data')


# In[28]:


default_args = {
    'owner': 'v-maslov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 11),
    'schedule_interval': '33 5 * * *'
}
dag = DAG('v-maslov-20-hw', default_args=default_args)


# In[29]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_doms',
                    python_callable=get_top10_doms,
                    dag=dag)

t3 = PythonOperator(task_id='maxlendom',
                        python_callable=maxlendom,
                        dag=dag)

t4 = PythonOperator(task_id='airflowplace',
                        python_callable=airflowplace,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[30]:


t1 >> [t2, t3, t4] >> t5


# In[ ]:




