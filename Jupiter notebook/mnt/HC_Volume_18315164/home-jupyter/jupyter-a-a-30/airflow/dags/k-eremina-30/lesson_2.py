#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime


# In[2]:


from airflow import DAG
from airflow.operators.python import PythonOperator


# In[3]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[4]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])    
    top_data_df['domain_zone']=top_data_df['domain'].str.split('.').str[1]
    top_domain_zone_top_10 = top_data_df.groupby('domain_zone', as_index = False)         .agg({'domain':'count'})         .sort_values('domain', ascending = False).head(10)
    with open('top_domain_zone_top_10.csv', 'w') as f:
        f.write(top_domain_zone_top_10.to_csv(index=False, header=False))


def get_max_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name']=top_data_df['domain'].apply(lambda x: x.split('.')[0])
    top_data_df['len_of_domain_name']=top_data_df['domain_name'].apply(lambda x: len(x))
    top_data_len_domain = top_data_df.sort_values(['len_of_domain_name', 'domain_name'], ascending=( False , True ))                                         .reset_index()                                         .iloc[[0]]
    with open('top_data_len_domain.csv', 'w') as f:
        f.write(top_data_len_domain.to_csv(index=False, header=False))

        
def get_airflow_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name']=top_data_df['domain'].apply(lambda x: x.split('.')[0])
    top_data_df['len_of_domain_name']=top_data_df['domain_name'].apply(lambda x: len(x))
    top_data_len_domain = top_data_df.sort_values(['len_of_domain_name', 'domain_name'], ascending=( False , True )).reset_index()
    airflow_com = top_data_len_domain.index[top_data_len_domain['domain'] == "airflow.com"][0]
    with open('airflow_com.txt', 'w') as f:
        f.write(str(airflow_com))


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_domain_zone_top_10.csv', 'r') as f:
        all_data_domain = f.read()
    with open('top_data_len_domain.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow_com.txt', 'r') as f:
        all_data_airflow = f.read() 
    date = ds
    

    print(f'Top domain zones for date {date}')
    print(all_data_domain)

    print(f'Top lengths in domain name for date {date}')
    print(all_data_len)
    
    print(f'Domain airflow.com for date {date}')
    print(all_data_airflow)


# In[5]:


default_args = {
    'owner': 'k-eremina-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 25),
    'schedule_interval': '0 13 * * *'
}
dag = DAG('k-eremina-30', default_args=default_args)


# In[6]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_domain_zone',
                    python_callable=get_domain_zone,
                    dag=dag)

t2_len = PythonOperator(task_id='get_max_len_domain',
                        python_callable=get_max_len_domain,
                        dag=dag)

t2_af = PythonOperator(task_id='get_airflow_com',
                    python_callable=get_airflow_com,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[7]:


t1 >> [t2_zone, t2_len, t2_af] >> t3


# In[ ]:




