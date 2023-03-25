#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_stat_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10_com.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))

# !!!!!!!! Найти топ-10 доменных зон по численности доменов
def get_stat_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone']=top_data_df['domain'].apply(lambda x: x.split(".")[-1])
    top_data_zone_top_10=top_data_df.groupby(['zone']).agg({'domain':'count'}).rename(columns={'domain':'count'}).sort_values('count', ascending=False).head(10)
    with open('top_data_zone_top_10.csv', 'w') as f:
        f.write(top_data_zone_top_10.to_csv(index=False, header=False))
                
# !!!!!!!! Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_stat_top_1_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len']=top_data_df['domain'].apply(lambda x: len(x))
    domain_len_top_1=top_data_df.sort_values(['domain_len','domain'], ascending=[False, True]).head(1).domain
    with open('domain_len_top_1.csv', 'w') as f:
        f.write(domain_len_top_1.to_csv(index=False, header=False))
        
        
# !!!!!!!!На каком месте находится домен airflow.com

def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_airflow=top_data_df[top_data_df.domain=='airflow.com']['rank']
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False))



def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_top_10_com.csv', 'r') as f:
        all_data_com = f.read()
    date = ds

    print(f'Top domains in .RU for date {date}')
    print(all_data)

    print(f'Top domains in .COM for date {date}')
    print(all_data_com)


default_args = {
    'owner': 'u.averianova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 21),
}
schedule_interval = '0 21 * * *'

dag = DAG('top_10_ru_new_averianova_ua', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2_zone=PythonOperator(task_id='get_stat_zone',
                    python_callable=get_stat_zone,
                    dag=dag)

t2_len=PythonOperator(task_id='get_stat_top_1_len',
                    python_callable=get_stat_top_1_len,
                    dag=dag)

t2_air=PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1>> [t2_zone, t2_len, t2_air]  >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)


# In[ ]:




