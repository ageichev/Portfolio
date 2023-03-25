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

def top_10_domain_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, header = None, names = ['index', 'doms'])
    df['domain_zone']=df.doms.str.split('.')
    df['domain_zone'] = df.domain_zone.apply(lambda x: list(x)[-1])
    top_10_domain_zones = df.groupby('domain_zone', as_index = False).doms.count()                        .sort_values('doms', ascending = False)                        .reset_index().drop('index', axis=1).head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))

def the_longest_dom_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, header = None, names = ['index', 'doms'])
    df['length'] = df.doms.apply(lambda x: len(x))
    the_longest_dom_is=df.sort_values('length', ascending = False).head(1)
    with open('the_longest_dom_name.txt', 'w') as f:
        f.write(str(the_longest_dom_is.doms).split()[1])

def airflow_place():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, header = None, names = ['index', 'doms'])
    AF_place = df.query('doms == "airflow.com"')['index']
    if AF_place.array.size > 0:
        with open('airflow_place_is.txt', 'w') as f:
            f.write(f'The place is the {int(AF_place)}')
    else:
        with open('airflow_place_is.txt', 'w') as f:
            f.write('Airlow.com is not in the list')


def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
    with open('the_longest_dom_name.txt', 'r') as f:
        longest_name = f.read()
    with open('airflow_place_is.txt', 'r') as f:
        airflow_place_is = f.read()
    date = ds

    print(f'Top-10 domain zones at {date}')
    print(top_10_domain_zones)

    print(f'The longest domain name as of {date} is')
    print(longest_name)
    
    print(f'As of {date} the situation is as follows:')
    print(airflow_place_is)


# In[4]:


default_args = {
    'owner': 'l-vasilets-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 5),
}
schedule_interval = '30 1 * * *'


# In[5]:


dag = DAG('l-vasilets-20_HW_Lesson2', default_args=default_args, schedule_interval=schedule_interval)


# In[6]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zones',
                    python_callable=top_10_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='the_longest_dom_name',
                        python_callable=the_longest_dom_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_place',
                        python_callable=airflow_place,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)


# In[ ]:




