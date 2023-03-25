#!/usr/bin/env python
# coding: utf-8

# In[16]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[17]:


#исходные данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[18]:


#получение данных       
def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[19]:


# Найти топ-10 доменных зон по численности доменов
def get_top10_domzone_s_seleznev():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top10_domzone = top_data_df.domain.str.split(".").str[-1].value_counts().head(10).reset_index()['index']
    with open('top10_domzone_s_seleznev.csv', 'w') as f:
        f.write(top10_domzone.to_csv(index=False, header=False))


# In[ ]:





# In[20]:


# самое длинное имя
def get_lname_s_seleznev():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    maxlen = top_data_df.domain.str.len().max()
    maxlendom = top_data_df[top_data_df.domain.str.len() == maxlen].sort_values(by=['domain']).head(1).reset_index().domain
    with open('lname_s_seleznev.csv', 'w') as f:
        f.write(maxlendom.to_csv(index=False, header=False))


# In[21]:


#На каком месте находится домен airflow.com?
def get_position_s_seleznev():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_dom = top_data_df.query('domain == "airflow.com"').count().iloc[0]
    if df_dom > 0:
        rank_dom = top_data_df.query('domain == "airflow.com"')['rank'].iloc[0]
    else:
        rank_dom = 'airflow.com нет в списке'
    
    with open('position_s_seleznev.csv', 'w') as f:
        f.write(rank_dom)


# In[22]:


# вывод результатов
def output_info(ds):
    with open('top10_domzone_s_seleznev.csv', 'r') as f:
        top_10_z = f.read()
    with open('lname_s_seleznev.csv', 'r') as f:
        lname = f.read()
    with open('position_s_seleznev.csv', 'r') as f:
        position = f.read()
    date = ds

    print(f'ТОП 10 доменных зон на {date}')
    print(top_10_z)
    
    print(f'Максимально длинное имя на {date}')
    print(lname)
    
    print(f'Позиция сайта Airflow на {date} это')
    print(position)


# In[23]:


default_args = {
    'owner': 's-seleznev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 11, 22),
    'schedule_interval': '0 10 * * *'
}


# In[24]:


dag = DAG('s-seleznev', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='get_top10_domzone_s_seleznev',
                    python_callable=get_top10_domzone_s_seleznev,
                    dag=dag)
                       
t2_lname = PythonOperator(task_id='get_lname_s_seleznev',
                    python_callable=get_lname_s_seleznev,
                    dag=dag)

t2_pos_af = PythonOperator(task_id='get_position_s_seleznev',
                    python_callable=get_position_s_seleznev,
                    dag=dag)


t3 = PythonOperator(task_id='output_info',
                    python_callable=output_info,
                    dag=dag)

t1 >> [t2_top10, t2_lname, t2_pos_af] >> t3


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[1]:





# In[ ]:





# In[ ]:




