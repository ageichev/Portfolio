#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
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
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# def get_stat():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))


# def get_stat_com():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10_com.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))

def top_10_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_dom = top_data_df['domain'].apply(lambda x: x.rsplit('.', 1)[1]).value_counts().head(10).to_frame()
    with open('top_10_dom.csv', 'w') as f:
        f.write(top_10_dom.to_csv( header=False))
        
def big_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_dom'] = top_data_df['domain'].apply(lambda x: len(x))
    big_dom = top_data_df.sort_values(['len_dom', 'domain'], ascending=[False, True]).head(1)['domain']
    with open('big_dom.csv', 'w') as f:
        f.write(big_dom.to_csv(index=False, header=False))

def airflow_inf():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_inf = top_data_df[top_data_df.domain == 'airflow.com'].rank
    No_inf = 'No_inf'
    try:
        if len(airflow_inf) > 0:
            with open('airflow_inf.csv', 'w') as f:
                f.write(airflow_inf.to_csv(index=False, header=False))
    except:
        with open('airflow_inf.txt', 'w') as f:
            f.write(No_inf)
        


def print_data(ds):
    with open('top_10_dom.csv', 'r') as f:
        top_10_dom = f.read()
    with open('big_dom.csv', 'r') as f:
        big_dom = f.read()
    try:
        with open('airflow_inf.csv', 'r') as f:
            airflow_inf = f.read()
    except:
        with open('airflow_inf.txt', 'r') as f:
            airflow_inf = f.read()
    date = ds

    print(f'Top domains for date {date}')
    print(top_10_dom)

    print(f'The longest domains in .COM for date {date}')
    print(big_dom)
    
    print(f'The longest domains in .COM for date {date}')
    print(airflow_inf)




# In[4]:


default_args = {
    'owner': 'd_khanin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 4),
}
schedule_interval = '0 14 * * *'

dag = DAG('d_khanin_top_10_ru_new', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

# t2 = PythonOperator(task_id='get_stat',
#                     python_callable=get_stat,
#                     dag=dag)

# t2_com = PythonOperator(task_id='get_stat_com',
#                         python_callable=get_stat_com,
#                         dag=dag)

t2_top_10_dom = PythonOperator(task_id='get_stat',
                    python_callable=top_10_dom,
                    dag=dag)

t2_big_dom = PythonOperator(task_id='big_dom',
                    python_callable=top_10_dom,
                    dag=dag)

t2_airflow_inf = PythonOperator(task_id='airflow_inf',
                    python_callable=top_10_dom,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_10_dom, t2_big_dom, t2_airflow_inf] >> t3


# In[ ]:




