#!/usr/bin/env python
# coding: utf-8

# In[48]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram
import sys
import os


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# Адрес рабочего датасета
sales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-novikov-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 22),
    'schedule_interval': '0 10 */3 * *'
}

# Определим год, за какой будем смотреть данные
year = 1994 + hash(f'a-novikov-21') % 23

# Настроим отправку уведомлений в чат-бота
CHAT_ID = 264188898
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass
    
@dag(default_args=default_args, catchup=False)
def a_novikov_21_lesson3():
    @task()
    def get_data():
        df = pd.read_csv(sales)
        return df
    
    @task()
    def most_sold(df):
        most_sold = df.query("Year == @year").groupby('Name').sum().Global_Sales.idxmax()
        return most_sold
    
    @task()
    def most_popular_eu_genre(df):
        most_popular_eu_genre = df.query("Year == @year").groupby('Genre').sum().EU_Sales.idxmax()
        return most_popular_eu_genre
    
    @task()
    def most_popular_na_platform(df):
        most_popular_na_platform = df.query("Year == @year & NA_Sales > 1").groupby('Platform').sum().NA_Sales.idxmax()
        return most_popular_na_platform
    
    @task()
    def most_popular_jp_publisher(df):
        most_popular_jp_publisher = df.query("Year == @year").groupby('Publisher').mean().JP_Sales.idxmax()
        return most_popular_jp_publisher
    
    @task()
    def games_eu_vs_jp(df):
        games_eu_vs_jp = (df.query("Year == @year").EU_Sales > df.query("Year == @year").JP_Sales).sum()
        return games_eu_vs_jp
    
    @task(on_success_callback=send_message)
    def print_data(most_sold, most_popular_eu_genre, most_popular_na_platform, most_popular_jp_publisher, games_eu_vs_jp):
        context = get_current_context()
        
        print(f'''
            Best selling game worldwide in {year}: {most_sold}
            Most popular genre in EU in {year}: {most_popular_eu_genre}
            Most popular platform in North America in {year}: {most_popular_na_platform}
            Best selling publisher in Japan in {year}: {most_popular_jp_publisher}
            More bestsellers in EU than in Japan in {year}: {games_eu_vs_jp}''')
        
    df = get_data()
    most_sold = most_sold(df)
    most_popular_eu_genre = most_popular_eu_genre(df)
    most_popular_na_platform = most_popular_na_platform(df)
    most_popular_jp_publisher = most_popular_jp_publisher(df)
    games_eu_vs_jp = games_eu_vs_jp(df)
    print_data(most_sold, most_popular_eu_genre, most_popular_na_platform, most_popular_jp_publisher, games_eu_vs_jp)
 
a_novikov_21_lesson3 = a_novikov_21_lesson3() 

