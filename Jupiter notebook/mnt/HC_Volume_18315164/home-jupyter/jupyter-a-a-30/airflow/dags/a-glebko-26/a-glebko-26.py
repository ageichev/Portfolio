#!/usr/bin/env python
# coding: utf-8

# In[8]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime


# In[9]:


from airflow import DAG
from airflow.operators.python import PythonOperator


# In[10]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[11]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[15]:


# Найти топ-10 доменных зон по численности доменов
def get_domain_zones():
    all_table = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    all_table['zone'] = all_table.domain.str.split('\.')
    all_table['zone'] = all_table['zone'].apply(lambda x : x[-1])
    top_domain_zones = all_table.groupby('zone', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10).zone.to_list()
    top_domain_zones = ', '.join(top_domain_zones)
    
    with open('ag_top_10_domain_zones.csv', 'w') as f:
        f.write(top_domain_zones)


# In[39]:


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def longest_damain():
    all_table = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) 
    all_table['lenght'] = all_table.domain.str.len()
    all_table = all_table.sort_values(['lenght','domain'], ascending=[False, True])
    longest_domain = all_table['domain'][all_table['lenght'].idxmax()]
    
    with open('ag_longest_domain_zones.csv', 'w') as f:
        f.write(longest_domain)


# In[42]:


# На каком месте находится домен airflow.com?
def airflow_pos():
    all_table = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    air_pos = all_table.loc[all_table.domain == 'airflow.de'].index.to_list()
    if len(air_pos) > 0:
        with open('ag_airflow_pos.csv', 'w') as f:
            f.write(air_pos[0].to_csv(index=False, header = False))
    else:
        with open('ag_airflow_pos.csv', 'w') as f:
            f.write('airflow.com isn\'t in list')


# In[43]:


def print_data(ds):
    with open('ag_top_10_domain_zones.csv', 'r') as f:
        top_domain_zone = f.read()
    with open('ag_longest_domain_zones.csv', 'r') as f:
        longest_dom = f.read()
    with open('ag_airflow_pos.csv', 'r') as f:
        airflow_position = f.read()
        
    date = ds

    print(f'Top domain zones for date {date}')
    print(top_domain_zone)

    print(f'Longest domain for date {date}')
    print(longest_dom)
    
    print(f'Airflow.com position for date {date}')
    print(airflow_position)


# In[44]:


default_args = {
    'owner': 'a.glebko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 17),
}
schedule_interval = '21 21 * * *'

dag = DAG('glebko_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_domain_zones',
                    python_callable=get_domain_zones,
                    dag=dag)

t2_len = PythonOperator(task_id='longest_damain',
                        python_callable=longest_damain,
                        dag=dag)

t2_air = PythonOperator(task_id='airflow_pos',
                        python_callable=airflow_pos,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_len, t2_air] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)


# In[ ]:




