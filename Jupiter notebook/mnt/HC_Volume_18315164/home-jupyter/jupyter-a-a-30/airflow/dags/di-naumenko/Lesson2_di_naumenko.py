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


# In[15]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_domen_10 = top_data_df[top_data_df['domain'].str.split('.')[-1]]
    top_data_domen_10 = top_data_domen_10.groupby('domain', as_index= False)                                        .agg({'rank':'count'})                                        .sort_values('rank', ascending = False)
    top_data_domen_10 = top_data_domen_10['domain'].head(10)
    
    with open('top_data_domen_10.csv', 'w') as f:
        f.write(top_data_domen_10.to_csv(index=False, header=False))
        
def get_stat_longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain['domain_length'] = top_data_df['domain'].apply(len)
    longest_domain['domain', 'domain_length'].sort_values('domain_length', ascending= False)                                            .reset_index()                                            .loc[0]['domain']
    
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

def get_stat_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    
    with open('airflow_rank.csv', 'w') as f:
        if top_data_df[top_data_df['domain']== 'airflow.com'].empty:
            f.write('airflow.com is not found')
        else: 
            airflow_rank = top_data_df[top_data_df['domain']== 'airflow.com']['rank']
            f.write(airflow_rank)

    
def print_data(ds): # передаем глобальную переменную airflow
    
    with open('top_data_domen_10.csv', 'r') as f:
        domen_data = f.read()
    
    with open('longest_domain.csv', 'r') as f:
        longest_domain_data = f.read()
    
    with open('airflow_rank', 'r') as f:
        airflow_rank_data = f.read()    
    date = ds

    print(f'Top 10 domain',"'",'s regions,"\", {date}')
    print(domen_data)

    print(f'The longest domain is: {date}')
    print(longest_domain_data)
          
    print(f'Airflow.com rank is: {date}')
    print(airflow_rank_data)


# In[16]:


default_args = {
    'owner': 'di-naumenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2022, 12, 2),
    'schedule_interval': '0 9 * * *'
}
dag = DAG('di_naumenko', default_args=default_args)


# In[17]:


t1 = PythonOperator(task_id='get_data', # Название таска
                    python_callable=get_data, # Название функции
                    dag=dag) # Параметры DAG

t2 = PythonOperator(task_id='get_stat', # Название таска
                    python_callable=get_stat, # Название функции
                    dag=dag) # Параметры DAG

t3 = PythonOperator(task_id='get_stat_longest', # Название таска
                    python_callable=get_stat_longest, # Название функции
                    dag=dag) # Параметры DAG

t4 = PythonOperator(task_id='get_stat_airflow', # Название таска
                    python_callable=get_stat_airflow, # Название функции
                    dag=dag) # Параметры DAG

t5 = PythonOperator(task_id='print_data', # Название таска
                    python_callable=print_data, # Название функции
                    dag=dag) # Параметры DAG


# In[18]:


t1 >> [t2, t3, t4] >> t5


# In[ ]:




