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

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_10_dom():

    d_dom = {}
    
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    for x in top_doms['domain'].values:
        if x.split('.')[1] in d_dom:
            d_dom[x.split('.')[1]] += 1
        else:
            d_dom[x.split('.')[1]] = 0

    
    sorted_tuples = sorted(d_dom.items(), key=lambda item: item[1], reverse = True)
    sorted_dict = {k: v for k, v in sorted_tuples}
    
    dic_items = sorted_dict.items()
    first_10 = list(dic_items)[:10]
    
    with open('top_10_dom.txt', 'w') as f:
        for x in first_10:
            print(x[0], x[1], file=f)


def longest_dom():
    
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_dom = sorted(top_doms['domain'].values, reverse=True, key=len)
    l = []
    for i, j in enumerate(longest_dom[:-1]):
        if j  > longest_dom[i+1] and len(l) == 0: 
            l.append(j)
            break

        if j == longest_dom[i+1]:
            l.append(j)
            l.append(longest_dom[i+1])

    l.sort()
    with open('the_longest_dom.txt', 'w') as f:
        print(l[0], file=f)

def get_position():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_pos = top_doms[top_doms['domain'].str.contains('airflow.com')].shape[0]
    if  airflow_pos > 0:
        with open('airflow.txt', 'w') as f:
            print(pos, file=f)
    else:
        with open('airflow.txt', 'w') as f:
            print('Данного домена нет в базе данных', file=f)


def print_data(ds):
    with open('top_10_dom.txt', 'r') as f:
        top10 = f.read()
    with open('the_longest_dom.txt', 'r') as f:
        longest_dom = f.read()
    with open('airflow.txt', 'r') as f:
        pos_airflow = f.read()
        
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top10)

    print(f'The longest domain for date {date}')
    print(longest_dom)
    
    print(f'Airflow position for date {date}')
    print(pos_airflow)

default_args = {
    'owner': 's-kolesov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=50),
    'start_date': datetime(2022, 7, 3),
}
schedule_interval = '0 12 * * *'

dag = DAG('s-kolesov_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_dom',
                    python_callable=top_10_dom,
                    dag=dag)

t3 = PythonOperator(task_id='longest_dom',
                        python_callable=longest_dom,
                        dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4


# In[ ]:




