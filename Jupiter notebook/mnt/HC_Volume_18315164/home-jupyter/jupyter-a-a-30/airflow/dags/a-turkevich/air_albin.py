#!/usr/bin/env python
# coding: utf-8

# In[75]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[76]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[77]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_name'] = [len(x) for x in top_data_df.domain]
    top_data_df_len_name = top_data_df.sort_values('len_name', ascending=False).reset_index()
    long_name = pd.DataFrame(['домен с самым длинным именем',top_data_df_len_name.iloc[0].domain])  
    with open('top_data_top_10_com.csv', 'w') as f:
        f.write(long_name.to_csv(index=False, header=False))

#Найти топ-10 доменных зон по численности доменов
def get_stat_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = [x.split('.')[1] for x in top_data_df.domain]    
    top_data_top_10 = top_data_df.zone.value_counts().head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=True, header=False, sep=':'))
        
def get_stat_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_name'] = [len(x) for x in top_data_df.domain]
    top_data_df_len_name = top_data_df.sort_values('len_name', ascending=False).reset_index()
    pd_airflow = top_data_df_len_name[top_data_df_len_name['domain']=='airflow.com'] 
    if len(pd_airflow)==0: 
        with open('top_airflow.csv', 'w') as f:
            f.write('нет домена airflow.com в списке')
            
    else: 
        with open('top_airflow.csv', 'w') as f:
            f.write(long_name.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_top_10_com.csv', 'r') as f:
        all_data_com = f.read()
    with open('top_airflow.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'топ-10 доменных зон по численности доменов')
    print(all_data)   
    print(all_data_com)
    print(all_data_airflow)


# In[78]:


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '0 12 * * *'

dag = DAG('a-turkevich', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)

t2_com = PythonOperator(task_id='get_stat_com',
                        python_callable=get_stat_com,
                        dag=dag)
t2_air = PythonOperator(task_id='get_stat_airflow',
                        python_callable=get_stat_airflow,
                        dag=dag)
t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[79]:


t1 >> [t2, t2_com, t2_air] >> t3

