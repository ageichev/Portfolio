#!/usr/bin/env python
# coding: utf-8

# Используем Airflow для решения аналитических задач.
# 
# Сначала определим год, за какой будем смотреть данные. Сделать это можно так: в питоне выполнить 1994 + hash(f‘{login}') % 23,  где {login} - ваш логин (или же папка с дагами)
# 
# Дальше нужно составить DAG из нескольких тасок, в результате которого нужно будет найти ответы на следующие вопросы:
# 
# Какая игра была самой продаваемой в этом году во всем мире?
# 
# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
# 
# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
# Перечислить все, если их несколько
# 
# У какого издателя самые высокие средние продажи в Японии?
# Перечислить все, если их несколько
# 
# Сколько игр продались лучше в Европе, чем в Японии?
# 
# Оформлять DAG можно как угодно, важно чтобы финальный таск писал в лог ответ на каждый вопрос. Ожидается, что в DAG будет 7 тасков. По одному на каждый вопрос, таск с загрузкой данных и финальный таск который собирает все ответы. Дополнительный бонус за настройку отправки сообщений в телеграмм по окончанию работы DAG

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[2]:


default_args = {
    'owner': 'o-ogurtsova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 28),
    'schedule_interval': '1 12 * * *'
}


# In[3]:


#определим год, за который будем смотреть данные
my_year = 1994 + hash('o-ogurtsova') % 23


# In[ ]:


# доступ в телеграм
CHAT_ID = -1001511167818
try:
    BOT_TOKEN = '5187981501:AAGdc6w9UlyhX38IspLxayXajDJIwxlg7cg'
except:
    BOT_TOKEN = ''


# In[ ]:


#сообщение в телеграм
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} with data for {my_year} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


# In[4]:


@dag(default_args=default_args, catchup=False)
def top_games_o_ogurtsova_tg():
    #Получение данных и фильтрация по году
    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df.Year = df.Year.fillna('0').astype(int)
        df_my_year = df.query('Year == @my_year')
        return df_my_year

    #Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_selling_game_world(df_my_year):
        best_selling_game_world = df_my_year.drop_duplicates(subset=['Year'])['Name'].tolist()
        return best_selling_game_world

    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_best_selling_genre_Europe(df_my_year):
        max_EU_sales = df_my_year.groupby('Genre', as_index=False)            .agg({'EU_Sales': 'sum'})            .EU_Sales            .max()   
        best_selling_genre_Europe = df_my_year            .groupby('Genre', as_index=False)            .agg({'EU_Sales' : 'sum'})            .query('EU_Sales == @max_EU_sales')            .reset_index(drop=True)            ['Genre']
        return best_selling_genre_Europe

    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    #Перечислить все, если их несколько
    @task()
    def get_best_platform_million_copies_NA(df_my_year):
        max_NA_names = df_my_year.query('NA_Sales > 1')            .groupby('Platform', as_index = False)            .agg({'Name' : 'count'})            .Name            .max()
        best_platform_million_copies_NA = df_my_year.query('NA_Sales > 1')            .groupby('Platform', as_index = False)            .agg({'Name' : 'count'})            .query('Name == @max_NA_names')            .reset_index(drop=True)            ['Platform']
        return best_platform_million_copies_NA

    #У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_best_avg_sales_publisher_Japan(df_my_year):
        max_JP_avg_sales = df_my_year.groupby('Publisher', as_index = False)            .agg({'JP_Sales' : 'mean'})            .JP_Sales            .max()                                                      
        best_avg_sales_publisher_Japan = df_my_year.groupby('Publisher', as_index = False)            .agg({'JP_Sales' : 'mean'})            .query('JP_Sales == @max_JP_avg_sales')            .reset_index(drop=True)            ['Publisher']                                                    
        return best_avg_sales_publisher_Japan

    #Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_count_games_better_Europe_than_Japan(df_my_year):
        count_games_better_Europe_than_Japan = df_my_year.query('EU_Sales > JP_Sales').shape[0]
        return count_games_better_Europe_than_Japan
    
    #Печать результата
    @task(on_success_callback=send_message)
    def print_data(task1, task2, task3, task4, task5):
        
        print(f'''The best-selling game worldwide in {my_year} year: {task1}''')
        print(f'''The best-selling genre in Europe in {my_year} year: {task2}''')
        print(f'''This platform had the most games that sold more than a million copies in North America in {my_year} year: {task3}''')
        print(f'''This publisher has the highest average sales in Japan in {my_year} year: {task4}''')
        print(f'''How many games sold better in Europe than in Japan in {my_year} year: {task5}''')

    df_my_year = get_data()
    
    best_selling_game_world = get_best_selling_game_world(df_my_year)
    best_selling_genre_Europe = get_best_selling_genre_Europe(df_my_year)  
    best_platform_million_copies_NA = get_best_platform_million_copies_NA(df_my_year)
    best_avg_sales_publisher_Japan = get_best_avg_sales_publisher_Japan(df_my_year)
    count_games_better_Europe_than_Japan = get_count_games_better_Europe_than_Japan(df_my_year)

    print_data(best_selling_game_world, best_selling_genre_Europe, best_platform_million_copies_NA, best_avg_sales_publisher_Japan, count_games_better_Europe_than_Japan)


# In[5]:


top_games_o_ogurtsova_tg = top_games_o_ogurtsova_tg()

