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


# In[11]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        

        

def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    data_splitted=top_data_df.domain.str.split('.', expand=True)
    data_splitted.columns = ['domain_name', 'domain', 'domain1', 'domain2','domain3']

    data_splitted_top_10=data_splitted.groupby('domain').agg({"domain_name":"count"}).sort_values(by="domain_name",ascending=False).reset_index().head(10)

    with open('data_top_10_domen.csv', 'w') as f:
        f.write(data_splitted_top_10.to_csv(index=False, header=False))


def get_domen_dlina():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    data_with_length=top_data_df.sort_values('domain',key=lambda x:x.str.len(),ascending=False)
    data_with_length['length']=top_data_df.domain.str.len()
    max_length=data_with_length.length.max()

    data_with_max_length=data_with_length.query("length== @max_length")
    longest_name=data_with_max_length.sort_values('domain', ascending=True).head(1)
    
    with open('domen_dlina.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))
   

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    
    a=top_data_df.query('domain=="airflow.com"')

    if a.empty:
        b='Domen Airflow ne v spiske'
    else:
        b=a['rank']
    
    print(b)

    with open('airflow_rank.csv', 'w') as f:
        f.write(b.to_csv(index=False, header=False))
   



def print_data(ds):
    with open('data_top_10_domen.csv', 'r') as f:
        all_data = f.read()
    with open('domen_dlina.csv', 'r') as f:
        all_data_dlina = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_airflow = f.read()   
        
    date = ds

    print(f'Top domains by number for date {date}')
    print(all_data)

    print(f'Longest domain for date {date}')
    print(all_data_dlina)
    
    print(f'Rank of Airflow for date {date}')
    print(all_airflow)


# In[ ]:


default_args = {
    'owner': 'f.imamagzam',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=7),
    'start_date': datetime(2022, 9, 25),
}
schedule_interval = '0 6 * * *'

dag = DAG('b-imamagzam', default_args=default_args, schedule_interval=schedule_interval)


# In[ ]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)

t3 = PythonOperator(task_id='get_domen_dlina',
                        python_callable=get_domen_dlina,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=print_data,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >>t3 >> t4>>t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

