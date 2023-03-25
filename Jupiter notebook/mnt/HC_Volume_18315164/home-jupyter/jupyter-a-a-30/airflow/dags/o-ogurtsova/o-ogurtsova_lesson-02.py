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


# In[3]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[4]:


def get_top10_zones_by_number():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top10_zones_by_number = top_data_df['domain'].str.extract('.+\.(.\w+)').value_counts().to_frame()
    top_data_top10_zones_by_number = top_data_top10_zones_by_number.head(10)
    with open('top_data_top10_zones_by_number.csv', 'w') as f:
        f.write(top_data_top10_zones_by_number.to_csv(header=False))


# In[5]:


def get_longest_name_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].str.count('')
    top_data_longest_name_domain = top_data_df.sort_values('domain_length', ascending=False).head(1)['domain']
    with open('top_data_longest_name_domain.csv', 'w') as f:
        f.write(top_data_longest_name_domain.to_csv(index=False, header=False))


# In[6]:


def get_place_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_place_airflow = top_data_df[top_data_df['domain'] == 'airflow.com']
    top_data_place_airflow['rank'] = [top_data_place_airflow['rank'] if len(top_data_place_airflow) > 0 else 'This domain is not in the rating']
    with open('top_data_place_airflow.csv', 'w') as f:
        f.write(top_data_place_airflow.to_csv(index=False, header=False))


# In[7]:


def print_data(ds):
    with open('top_data_top10_zones_by_number.csv', 'r') as f:
        all_data_top10_zones_by_number = f.read()
    with open('top_data_longest_name_domain.csv', 'r') as f:
        all_data_longest_domain = f.read()
    with open('top_data_place_airflow.csv', 'r') as f:
        all_data_place_airflow = f.read()
    date = ds

    print(f'Top 10 domain zones by number of domains for date {date}')
    print(all_data_top10_zones_by_number)

    print(f'The domain with the longest name for date {date}')
    print(all_data_longest_domain)
    
    print(f'Where is the domain located airflow.com for date {date}')
    print(all_data_place_airflow)


# In[8]:


default_args = {
    'owner': 'o-ogurtsova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 19),
}
schedule_interval = '1 11 * * *'

dag = DAG('statistics_by_domains_from_o-ogurtsova', default_args=default_args, schedule_interval=schedule_interval)


# In[9]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='get_top10_zones_by_number',
                    python_callable=get_top10_zones_by_number,
                    dag=dag)

t2_longest_domain = PythonOperator(task_id='get_longest_name_domain',
                        python_callable=get_longest_name_domain,
                        dag=dag)

t2_place_airflow = PythonOperator(task_id='get_place_airflow',
                        python_callable=get_place_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[10]:


t1 >> [t2_top10, t2_longest_domain, t2_place_airflow] >> t3

