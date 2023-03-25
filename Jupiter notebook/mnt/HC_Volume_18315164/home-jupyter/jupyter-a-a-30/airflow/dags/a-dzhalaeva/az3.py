#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[5]:


path = '/mnt/HC_Volume_18315164/home-jupyter/jupiter-a-dzhalaeva'  
login = 'a-dzhalaeva'
year = 1994 + hash(f'{login}') % 23

CHAT_ID = -620798068
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''


# In[6]:


df = pd.read_csv(path)


# In[6]:


#Пишем функцию которая будет отправлять сообщения в чат телеграм
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Very good! Dag {dag_id} is completed on {date}.'
    params = {'chat_id': CHAT_ID, 'text': message}
    base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)
                     
default_args = {
    'owner': 'a.dzhalaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'schedule_interval' : '0 7 * * *',
}


# In[7]:


def answers():
                     
    #Считали и вернули таблицу
    @task(retries=3)
    def get_data():
        df = pd.read_csv(path)
        df = df.query("Year == @year")
        return df 
    


# In[8]:


@dag(default_args=default_args, catchup=False)
def answers():
    @task(retries=3)
# Cчитываем таблицу
    def get_data():
        df = pd.read_csv(path)
        df = df.query("Year == @year")
        return df 
# Какая игра была самой продаваемой в этом году во всем мире?   
    @task(retries=4, retry_delay=timedelta(10))
    def top_game(df):
        top_game_is = df.groupby('Name', as_index=False)         .agg({'Global_Sales':'sum'})         .sort_values(by='Total_Sales', ascending=False)         .max()
        return top_game_is
# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def top_in_EU(df):
        top_in_EU_is = df.groupby('Genre', as_index=False)         .agg({'EU_Sales':'sum'})         .sort_values(by='EU_Sales', ascending=False)         .max()
        return top_in_EU_is
# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
# Перечислить все, если их несколько
    @task()
    def top_NA_Sales(df):
        NA_Sales_million_is = df.query('NA_Sales > 1')         .groupby(["Platform"], as_index=False)         .agg({"NA_Sales":"sum"})         .sort_values("NA_Sales", ascending=False)         .max()
        return NA_Sales_million_is
# У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def JP_top_sales(df):
        JP_pablisher_top__is = df.groupby(["Publisher"], as_index=False)         .agg({"JP_Sales":"sum"})         .sort_values("JP_Sales", ascending=False)        .max() 
        return JP_pablisher_top__is
#  Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def EU_vs_JP(df):
        vs_top = df.groupby(["Name"], as_index=False)         .agg({"JP_Sales":"sum", "EU_Sales":"sum"})         .query('EU_Sales > JP_Sales')         .shape[0]
        return vs_top

    @task(on_success_callback=send_message)
    def print_data(top_game_is, top_in_EU_is, NA_Sales_million_is, JP_pablisher_top__is, vs_top):

        context = get_current_context()
        date = context['ds']

        print(f' Какая игра была самой продаваемой в {date}г. во всем мире?')
        print(top_game_is)
        print(f' Игры какого жанра были самыми продаваемыми в {date}г. в Европе?')
        print(top_in_EU_is)
        print(f' На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в {date}г. в Северной Америке?')
        print(NA_Sales_million_is)
        print(f' У какого издателя самые высокие средние продажи в Японии за {date}г.?')
        print(JP_pablisher_top__is)
        print(f' Сколько игр продались лучше в Европе, чем в Японии за {date}г.?')
        print(vs_top)

    top_game_is = top_game(df)
    top_in_EU_is = top_in_EU(df)
    NA_Sales_million_is = top_NA_Sales(df)
    JP_pablisher_top__is = JP_top_sales(df)
    vs_top = EU_vs_JP(df)
    
    print_data(top_game_is, top_in_EU_is, NA_Sales_million_is, JP_pablisher_top__is, vs_top)
    
dag_answers_for_task = answers()


# In[ ]:




