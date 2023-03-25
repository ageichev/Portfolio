#!/usr/bin/env python
# coding: utf-8

# In[29]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# Забираем данные

# In[30]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# Прописываем сами ТАСКИ с помощью функций:

# In[49]:


def get_data_al_golovin():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[48]:


def get_stat_all_al_golovin():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_top_10_domain_zone = top_data_df        .groupby('domain_zone', as_index=False)        .agg({'rank':'count'})        .sort_values('rank', ascending=False)        .head(10)
    
    with open('top_domain_zone.csv', 'w') as f:
        f.write(top_data_top_10_domain_zone.to_csv(index=False, header=False))


# In[57]:


def get_stat_max_al_golovin():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_lenght'] = top_data_df['domain'].apply(len)
    max_length_domain_str = top_data_df[['domain', 'domain_lenght']].sort_values('domain_lenght', ascending=False).reset_index().loc[0]['domain']
    
    with open('max_length_domain_str.txt', 'w') as f:
        f.write(str(max_length_domain_str))


# In[58]:


get_stat_max_al_golovin()


# In[63]:


def get_stat_airflow_al_golovin():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']
    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


# In[20]:


def print_data_al_golovin(ds): # передаем глобальную переменную airflow
    with open('top_domain_zone.csv', 'r') as f:
        all_data = f.read()
    with open('max_length_domain_str.txt', 'r') as f:
        all_data_max = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)

    print(f'Longest domain name is: ')
    print(all_data_max)
    
    print(f'Rank of airflow.com is: ')
    print(all_data_airflow)


# Инициализируем DAG

# In[22]:


default_args = {
    'owner': 'al-golovin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 17),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('top_10_sites_al_golovin', default_args=default_args)


# Инициализируем таски:

# In[23]:


t1 = PythonOperator(task_id='get_data_al_golovin',
                    python_callable=get_data_al_golovin,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_all_al_golovin',
                    python_callable=get_stat_all_al_golovin,
                    dag=dag)

t2_max = PythonOperator(task_id='get_stat_max_al_golovin',
                        python_callable=get_stat_max_al_golovin,
                        dag=dag)
t2_air = PythonOperator(task_id='get_stat_airflow_al_golovin',
                        python_callable=get_stat_airflow_al_golovin,
                        dag=dag)

t3 = PythonOperator(task_id='print_data_al_golovin',
                    python_callable=print_data_al_golovin,
                    dag=dag)


# Задаем логику выполнения:

# In[24]:


# последовательность выполнения и зависимость тасков, простой способ
t1 >> [t2, t2_max, t2_air] >> t3


# In[25]:


# Методы таска, более сложный способ:

# t1.set_downstream(t2)
# t1.set_downstream(t2_com)

# t2.set_downstream(t3)
# t2_com.set_downstream(t3)


# In[ ]:




