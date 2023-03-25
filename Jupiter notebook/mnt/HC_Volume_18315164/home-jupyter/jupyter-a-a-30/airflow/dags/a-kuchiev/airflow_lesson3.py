#!/usr/bin/env python
# coding: utf-8

# In[8]:


import numpy as np
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.models import Variable

import requests
import json
from urllib.parse import urlencode

chat_id = '352062222'
try:
    bot_token = Variable.get('telegram_secret')
except:
    bot_token = '5720238625:AAEN6RFAVccbCRkSMn9J3LAKHYlQJzcypHg'
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Success {dag_id} completed on {date}'

    params = {'chat_id': chat_id, 'text': message}

    base_url = f'https://api.telegram.org/bot{bot_token}/'
    url = base_url + 'sendMessage?' + urlencode(params)

    resp = requests.get(url)

default_args = {
    'owner': 'a-kuchiev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 28),
    'schedule_interval' : '0 14 * * *'
}

@dag(default_args=default_args, catchup=False)
def home_work_3_a_kuchiev():
    @task(retries=3)
    def find_the_year(login='a-kuchiev'):
        my_year = 1994 + hash(f'{login}') % 23   
        return my_year

    @task()
    def task_1(my_year):
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df[df['Year'] == my_year] 
        task_1 = df.groupby('Name', as_index=False)['Global_Sales'].sum().sort_values(by='Global_Sales', ascending=False).head(1).values[0][0]
        return task_1

    @task(retries=4)
    def task_2(my_year):
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df[df['Year'] == my_year] 
        quantile = df.groupby('Name', as_index=False)['EU_Sales'].sum().sort_values(by='EU_Sales', ascending=False).quantile(0.99)[0]
        task_2 = df[df['EU_Sales'] > quantile]['Name'].values
        return task_2

    @task(retries=3)
    def task_3(my_year):
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df[df['Year'] == my_year] 
        task_3 = df.groupby('Platform', as_index=False)['NA_Sales'].sum().sort_values(by='NA_Sales', ascending=False).query("NA_Sales > 1")['Platform'].values
        return task_3

    @task()
    def task_4(my_year):
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df[df['Year'] == my_year] 
        task_4 = df.groupby('Platform', as_index=False).JP_Sales.mean().sort_values(by='JP_Sales', ascending=False).head(1)['Platform'].values
        return task_4

    @task()
    def task_5(my_year):
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df[df['Year'] == my_year]
        task_5 = df.groupby('Name', as_index=False).agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        task_5['compare'] = np.where(task_5['EU_Sales'] > task_5['JP_Sales'], task_5['EU_Sales'], np.nan)   
        task_5 = task_5.query("compare != 'NaN'").shape[0] 
        return task_5

    @task(on_success_callback=send_message)
    def print_data(my_year, task_1, task_2, task_3, task_4, task_5):
        
        context = get_current_context()
        data = context['ds']
        print(f'Day of routine check {data}')

        print(f'Which game was the best selling in {my_year} year worldwide?')
        print(task_1)

        print(f'Which genre of the game was the best selling at {my_year} in Europe?')
        print(task_2)

        print(f'What platform had more than million sales at {my_year} in NA?')
        print(task_3)

        print(f'Which publisher had the biggest average selles at {my_year} in JP?')
        print(task_4)

        print(f'How many games were sold the better at {my_year} in Europe than in JP?')
        print(task_5)


    my_year = find_the_year()
    task_1 = task_1(my_year)
    task_2 = task_2(my_year)
    task_3 = task_3(my_year)
    task_4 = task_4(my_year)
    task_5 = task_5(my_year)
    print_data(my_year, task_1, task_2, task_3, task_4, task_5)

homework_3 = home_work_3_a_kuchiev()


# In[ ]:




