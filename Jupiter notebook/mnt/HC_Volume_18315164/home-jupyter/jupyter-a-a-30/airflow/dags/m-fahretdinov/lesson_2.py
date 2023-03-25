#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# Считываем данные

# In[2]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# Создаем функции для DAG

# In[3]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[4]:


def domain_top_10():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rang', 'domain'])
    df['top_dom'] = df['domain'].str.split('.').str[-1]
    df_top_10 = df.top_dom.value_counts().sort_values(ascending=False).reset_index().head(10)
    df_top_10.rename(columns = {'index' : 'domain', 'top_dom':'qty'}, inplace = True)
    with open('df_top_10.csv', 'w') as f:
        f.write(df_top_10.to_csv(index=False, header=False))


# In[5]:


def get_longest_dom():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rang', 'domain'])
    the_longest = max([(len(x),x) for x in (df['domain'])])
    with open('the_longest.txt', 'w') as f:
        f.write(the_longest.to_csv(index=False, header=False))


# In[6]:


def airflow_place():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rang', 'domain'])
    air = df.query('domain == "airflow.com"')
    with open('air.txt', 'w') as f:
        f.write(air.to_csv(index=False, header=False))


# In[ ]:





# Вывести все данные

# In[7]:


def print_data(ds):
    with open('df_top_10.csv', 'r') as f:
        df_top_10 = f.read()
    with open('the_longest.txt', 'r') as f:
        the_longest = f.read()
    with open('air.txt', 'r') as f:
        air = f.read()    
    date = ds
    
    print(f'Top domains zone by domain counts for date {date}')
    print(df_top_10)
    print(f'The longest domail name for date {date} is {the_longest}')
    print(f'Airfow rank for date {date} is {air}')


# In[ ]:





# DAG

# In[8]:


default_args = {
    'owner': 'm-fahretdinov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 18),
}
schedule_interval = '0 18 * * *'

dag = DAG('m-fahretdinov', default_args=default_args, schedule_interval=schedule_interval)


# In[9]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)


# In[10]:


t2 = PythonOperator(task_id='domain_top_10',
                    python_callable=domain_top_10,
                    dag=dag)


# In[11]:


t3 = PythonOperator(task_id='get_longest_dom',
                        python_callable=get_longest_dom,
                        dag=dag)


# In[12]:


t4 = PythonOperator(task_id='where_airflow',
                        python_callable=airflow_place,
                        dag=dag)


# In[13]:


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[14]:


t1 >> [t2, t3, t4] >> t5
#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




