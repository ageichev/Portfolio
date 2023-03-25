#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
#import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

FILE_VGSALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-shihanova-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 15),
    'schedule_interval': '30 9 * * *'
}

#CHAT_ID = -620798068
#try:
#   BOT_TOKEN = Variable.get('telegram_secret')
#except:
#    BOT_TOKEN = ''

#def send_message(context):
#    date = context['ds']
#    dag_id = context['dag'].dag_id
#    message = f'Huge success! Dag {dag_id} completed on {date}'
#    if BOT_TOKEN != '':
#        bot = telegram.Bot(token=BOT_TOKEN)
#        bot.send_message(chat_id=CHAT_ID, text=message)
#    else:
#        
year = 1994 + hash(f'{"a-shihanova-20"}') % 23

@dag(default_args=default_args, catchup=False)
def hw3_a_shihanova_20():
    
# Получаем данные
    @task(retries=4, retry_delay=timedelta(10))
    def get_data():
        #year = 1994 + hash(f'{"a-shihanova-20"}') % 23
        file = FILE_VGSALES
        db_game = pd.read_csv(file).query('Year == @year')
        return db_game

# Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_game_sale(db_game):
        best_game = db_game[db_game.Global_Sales == db_game.Global_Sales.max()].Name.values[0]
        return best_game

# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_best_genre_EU(db_game):
        best_genre = db_game[db_game.EU_Sales == db_game.EU_Sales.max()].Genre.to_list()
        return best_genre
    
# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def get_best_platform_NA(db_game):
        Platform_NA = db_game.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'NA_Sales': 'count'})
        best_Platform = Platform_NA[Platform_NA.NA_Sales == Platform_NA.NA_Sales.max()].Platform.to_list()
        return best_Platform

# У какого издателя самые высокие средние продажи в Японии?
    @task()
    def get_best_publisher_JP(db_game):
        Publisher = db_game.groupby('Publisher',as_index=False).agg({'JP_Sales':'mean'})
        best_Publisher = Publisher[Publisher['JP_Sales']==Publisher.JP_Sales.max()].Publisher.to_list()
        return best_Publisher

# Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_EU_sale_JP(db_game):
        EU_JP = db_game.groupby('Name',as_index=False).agg({'EU_Sales':sum, 'JP_Sales':sum})
        better_sale = EU_JP.query('EU_Sales > JP_Sales').shape[0]
        return better_sale

# Выводим на печать результаты
    @task()
    def print_data(best_game, best_genre, best_Platform, best_Publisher, better_sale):
        print(f'The best game sale in {year}:', best_game)
        print(f'The best genre EU in {year}:', best_genre)
        print(f'Top best platform NA in {year}:', best_Platform)
        print(f'The best publisher JP in {year}:', best_Publisher)
        print(f'The game better sale EU than JP in {year}:', better_sale)

    data = get_data()
    best_game_sale = get_best_game_sale(data)
    best_genre_EU = get_best_genre_EU(data)
    best_platform_NA = get_best_platform_NA(data)
    best_publisher_JP = get_best_publisher_JP(data)
    EU_better_sale_JP = get_EU_sale_JP(data)   

    print_data(best_game_sale, best_genre_EU, best_platform_NA, best_publisher_JP, EU_better_sale_JP)
    
hw3_a_shihanova_20 = hw3_a_shihanova_20()


# In[ ]:




