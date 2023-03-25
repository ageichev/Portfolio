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


# In[5]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[6]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_frame():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top = top_data_df.domain.str.rsplit('.', 1, expand = True)
    top.index = top.index + 1
    top = top.reset_index().rename(columns = {'index': 'rank', 0: 'site', 1: 'zone'})
    top['long'] = top.site.str.len()
    with open('top.csv', 'w') as f:
        f.write(top.to_csv(index=False, header=True))

        
def get_top_zone_10():
    top = pd.read_csv('top.csv')
    top_10 = top.groupby('zone').agg({'site': 'count'}).sort_values('site', ascending = False).head(10).reset_index()
    top_10.index = top_10.index + 1
    top_10 = top_10.reset_index().rename(columns = {'index': 'rank', 'site': 'count_site'})
    with open('top_zone_10.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=True))


def get_long_name_1():
    top = pd.read_csv('top.csv')
    long_name_1 = top.sort_values('long', ascending = False).head(1)
    with open('long_name_1.csv', 'w') as f:
        f.write(long_name_1.to_csv(index=False, header=True))

        
def get_airflow_com():
    top = pd.read_csv('top.csv')
    airflow_com = top[(top.site == 'airflow') & (top.zone == 'com')]
    with open('airflow_com.csv', 'w') as f:
        f.write(airflow_com.to_csv(index=False, header=True))

        
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_zone_10.csv', 'r') as f:
        top_zone = f.read()
    with open('long_name_1.csv', 'r') as f:
        long_name = f.read()
    with open('airflow_com.csv', 'r') as f:
        airflow = f.read()
    date = ds

    print(f'Top web zone for date {date}')
    print(top_zone)

    print(f'The longest name of web site for date {date}')
    print(long_name)

    print(f'The data about airflow.com {date}')
    print(airflow)


# In[7]:


default_args = {
    'owner': 'a-rajsih',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 27),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('top_new_aer', default_args=default_args)


# In[8]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_frame',
                    python_callable=get_frame,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_zone_10',
                        python_callable=get_top_zone_10,
                        dag=dag)

t4 = PythonOperator(task_id='get_long_name_1',
                        python_callable=get_long_name_1,
                        dag=dag)

t5 = PythonOperator(task_id='get_airflow_com',
                        python_callable=get_airflow_com,
                        dag=dag)

t6 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[9]:


t1 >> t2 >> [t3, t4, t5] >> t6


# In[8]:


# top = pd.read_csv('top-1m.csv', names = ['n', 'site'])
# top = top.site.str.rsplit('.', 1, expand = True)
# top.index = top.index + 1
# top = top.reset_index().rename(columns = {'index': 'rank', 0: 'site', 1: 'zone'})
# top['long'] = top.site.str.len()
# with open('top.csv', 'w') as f:
#     f.write(top.to_csv(index=False, header=True))


# In[9]:


# top = pd.read_csv('top.csv')
# top_10 = top.groupby('zone').agg({'site': 'count'}).sort_values('site', ascending = False).head(10).reset_index()
# top_10.index = top_10.index + 1
# top_10 = top_10.reset_index().rename(columns = {'index': 'rank', 'site': 'count_site'})
# with open('top_zone_10.csv', 'w') as f:
#     f.write(top_10.to_csv(index=False, header=True))


# In[4]:


#     with open('top_zone_10.csv', 'r') as f:
#         top_zone_10 = f.read()


# In[7]:


#     print(f'Top web zone for date ')
#     print(top_zone_10)


# In[25]:


# top.sort_values('long', ascending = False).head(1)


# In[26]:


# top[(top.site == 'airflow') & (top.zone == 'com')]


# In[ ]:




