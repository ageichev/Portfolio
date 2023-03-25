#!/usr/bin/env python
# coding: utf-8

# In[31]:


import requests  
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# In[32]:


TOP_1M_DOMAINS ='http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'


# In[33]:


TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[34]:


#считаем данные


# In[35]:


def get_my_filik():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[36]:


#Найти топ-10 доменных зон по численности доменов


# In[37]:


def find_zones():
    top_data_df  = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank','domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_zones = top_data_df.groupby('zone',as_index=False).agg({'domain':'nunique'}).sort_values(by='domain',ascending=False)
    top_zones = top_zones.rename(columns = {'domain' : 'count'})
    top_zones = top_zones.head(10)
    
    with open('top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))


# In[38]:


#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)


# In[39]:


def find_domen_with_max_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain_len = top_data_df.domain.apply(lambda x: len(x.split('.')[0])).max()
    longest_domain = top_data_df['domain'][top_data_df.domain.apply(lambda x: len(x.split('.')[0])==longest_domain_len)].sort_values().iloc[0]
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)


# In[40]:


#На каком месте находится домен airflow.com?


# In[41]:


def find_a_place_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df[top_data_df.domain == 'airflow.com'].shape[0]:
        place_airflow = top_data_df[top_data_df.domain == 'airflow.com'].iloc[0]['rank']
    else:
        place_airflow = 'No information'
    with open('place_airflow.txt', 'w') as f:
        f.write(str(place_airflow))


# In[42]:


#вывод доменов


# In[43]:


def print_all(ds):
    with open('top_zones.csv', 'r') as f:
        data_domain_zone = f.read()
    with open('longest_domain.txt', 'r') as f:
        data_domain_max = f.read()
    with open('place_airflow.txt', 'r') as f:
        data_rank_airflow = f.read()
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(top_zones)

    print(f'Max name domain for date {date}')
    print(longest_domain)
    
    print(f'Rank airflow for date {date}')
    print(place_airflow)


# In[44]:


default_args = {
    'owner': 'v_zimina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 21),
}
schedule_interval = '0 12 * * *'


# In[45]:


dag = DAG('v_zimina_last', default_args=default_args, schedule_interval=schedule_interval)


# In[46]:


t1 = PythonOperator(task_id='get_my_filik',
                    python_callable=get_my_filik,
                    dag=dag)

t2_zones = PythonOperator(task_id='find_zones',
                    python_callable=find_zones,
                    dag=dag)

t2_max_len = PythonOperator(task_id='find_domen_with_max_len',
                        python_callable=find_domen_with_max_len,
                        dag=dag)

t2_pl_ar = PythonOperator(task_id='find_a_place_airflow',
                        python_callable=find_a_place_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_all',
                    python_callable=print_all,
                    dag=dag)

t1 >> [t2_zones, t2_max_len, t2_pl_ar] >> t3


# In[ ]:




