#!/usr/bin/env python
# coding: utf-8

# Необходимо выполнить:
# 
# 1) Поменять имена dag на уникальные (лучше всего как-то использовать свой логин).
# 
# Поставить новую дату начала DAG и новый интервал (все еще должен быть ежедневным)
# 
# 2) Удалить таски get_stat и get_stat_com. Вместо них сделать свои собственные, которые
# 
# считают следующие:
# 
# Найти топ-10 доменных зон по численности доменов
# 
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
# 
# На каком месте находится домен airflow.com?
# 
# 3) Финальный таск должен писать в лог результат ответы на вопросы выше

# In[6]:


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

def get_start_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    top_data_top_10_zone=top_data_df[1].str.split('.', expand=True).iloc[:, [0, 1]]
    top_data_top_10_zone=top_data_top_10_zone.groupby(1, as_index=False).agg(number=(1,'count')).sort_values('number', ascending=False).head(10)
    with open('top_data_top_10_zone.csv', 'w') as f:
        f.write(top_data_top_10_zone.to_csv(index=False, header=False))

        
def get_start_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    top_data_length=top_data_df.copy()
    top_data_length['count']=top_data_length[1].str.len()
    top_data_length=top_data_length.sort_values('count', ascending=False).head(1)
    with open('top_data_length.csv', 'w') as f:
        f.write(top_data_length.to_csv(index=False, header=False))        
        
def get_airflow_rank():      
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    top_data_airflow=top_data_df.rename(columns={1:'domain'})
    top_data_airflow= top_data_airflow[top_data_airflow['domain'].str.contains("airflow")].rename(columns={0:'rank'})
    with open('top_data_airflow.csv', 'w') as f:
        f.write(top_data_airflow.to_csv(index=False, header=False))  

def print_data(ds):
    with open('top_data_top_10_zone.csv', 'r') as f:
        all_data_zone = f.read()
    with open('top_data_length.csv', 'r') as f:
        all_data_length = f.read()
    with open('top_data_airflow.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top 10 zone domains by number for date {date}')
    print(all_data_zone)

    print(f'Domain with the most length for date {date}')
    print(all_data_length)

    print(f"Airflow's domain for date {date}")
    print(all_data_airflow)

default_args = {
    'owner': 'i.inchenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 31),
}
schedule_interval = '0 9 * * *'

dag = DAG('homework', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_start_domain_zone',
                    python_callable=get_start_domain_zone,
                    dag=dag)

t2_length = PythonOperator(task_id='get_start_length',
                        python_callable=get_start_length,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_length, t2_airflow] >> t3


# In[50]:




