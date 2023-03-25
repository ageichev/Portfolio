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


# In[2]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[127]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank','domain'])
    top_data_df['domain2'] = [i.split('.')[1] for i in top_data_df['domain']]
    top_top_10 = top_data_df.groupby('domain2', as_index = False)                                 .agg({'rank' : 'count'})                                 .sort_values('rank',ascending = False)                                 .head(10)
    with open('top_top_10.csv', 'w') as f:
        f.write(top_top_10.to_csv(index=False, header= ['domain', 'count']))
        
def get_top_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].str.len()
    top_len = top_data_df.sort_values('length').tail(1)
    
    with open('top_len.csv', 'w') as f:
        f.write(top_len.to_csv(index=False, header=['rank', 'domain', 'length']))

def get_airflow_position():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if "airflow.com" in top_data_df.domain.to_list():
        airflow_position = top_data_df.query('domain == "airflow.com"')
    else:
        airflow_position = pd.DataFrame({'rank':'none', 'domain':['airflow.com']})
    
    with open('airflow_position.csv', 'w') as f:
        f.write(airflow_position.to_csv(index=False, header=['rank', 'domain']))
        
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_top_10.csv', 'r') as f:
        all_data_1 = f.read()
    with open('top_len.csv', 'r') as f:
        all_data_2 = f.read()
    with open('airflow_position.csv', 'r') as f:
        all_data_3 = f.read()
    date = ds

    print(f'Top domains for date {date}')
    print(all_data_1)

    print(f'Top name domain by length for date {date}')
    print(all_data_2)
    
    print(f'airflow.com position for date {date}')
    print(all_data_3)


# In[128]:


default_args = {
    'owner': 'an-gusarova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 24),
    'schedule_interval' : '0 12 * * *'
}

    
dag = DAG('top_domain_an-gusarova', default_args=default_args)


# In[129]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_len',
                        python_callable=get_top_len,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_position',
                        python_callable=get_airflow_position,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[130]:


t1 >> [t2, t3, t4] >> t5


# In[ ]:





# In[ ]:




