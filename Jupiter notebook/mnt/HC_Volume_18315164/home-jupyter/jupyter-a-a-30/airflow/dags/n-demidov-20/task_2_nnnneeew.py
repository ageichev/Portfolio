#!/usr/bin/env python
# coding: utf-8

# In[3]:


#!/usr/bin/env python
# coding: utf-8

# In[31]:

# импорт библиотек
import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
#  импорт библиотек airflow
from airflow import DAG
from airflow.operators.python import PythonOperator


# In[32]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[33]:

# получение данных
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data.to_csv)


# In[34]:

#  топ 10 доменных зон
def get_top_dom_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.apply(lambda x: x.split('.')[1])
    top_10_domain_zone = top_data_df.groupby('domain_zone', as_index=False).agg({'rank': 'count'}).sort_values('rank', ascending='False').domain_zone.head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))


# In[35]:

# поиск самого длинного домена
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.apply(lambda x: len(str(x).split('.')[0]))
    max_length = top_data_df.domain_length.agg('max')
    longest_domain = top_data_df.query('domain_length == @max_length').sort_values('domain').domain.iloc[0]
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))
    


# In[36]:

#  ранк airflow.com
def find_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query("domain == 'airflow.com'")['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

    


# In[37]:

# печать отчета
def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        all_data = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow = f.read()
    with open('longest_domain.csv', 'r') as f:
        long_dom = f.read()
    
    
    date = ds    
    print(f'Longest domain name is {long_dom}')

    print(f'Top-10 domain zone for date {date}')
    print(all_data)
    
    print(f'Airflow rank is {airflow}')


# In[38]:

# параметры дага
default_args = {
    
    'owner': 'n-demidov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 2),
}

schedule_interval = '0 6 * * *'


# In[39]:

# даг
dag = DAG('n_demidov_20_task_2', default_args=default_args, schedule_interval=schedule_interval)


# In[40]:


t1 = PythonOperator(task_id='get_top_dom_zone',
                    python_callable=get_top_dom_zone,
                    dag=dag)

t2 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t3 = PythonOperator(task_id='find_airflow_rank',
                        python_callable=find_airflow_rank,
                        dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[41]:


t1 >> [t2, t3] >> t4
# таски


# In[ ]:





# In[ ]:





# In[ ]:




