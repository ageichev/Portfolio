#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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



vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'imamagzam',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 2),
    'schedule_interval': '@daily'
}
my_year = 1994 + hash('b-imamagzam') % 23
CHAT_ID = 35156799
try:
    BOT_TOKEN = '5170502272:AAHtklYPUTewLzkY5EbBKFiRFMi2FyMgpok'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def imamagzam_dag_lesson_3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(vgsales).query('Year == @my_year')
        return df.to_csv(index=False)

    @task()
    def get_the_top_game_world(df):
        df = pd.read_csv(StringIO(df))
        the_top_game_world = df[df.Global_Sales == df.Global_Sales.max()].values[0][1]
        return the_top_game_world
    
    @task()
    def get_genre_eu_top(df):
        df = pd.read_csv(StringIO(df))
        genre_eu_sum = df.groupby('Genre').EU_Sales.sum()
        genre_eu_top = genre_eu_sum[genre_eu_sum == genre_eu_sum.max()].index.to_list()
        return genre_eu_top

    @task()
    def get_top_platform_na_sales(df):
        df = pd.read_csv(StringIO(df))
        platform_na_sales_more_mln = df[df.NA_Sales > 1].groupby('Platform').Name.count()
        platform_na_sales_more_mln_top = platform_na_sales_more_mln[platform_na_sales_more_mln 
                                                            == platform_na_sales_more_mln.max()].index.to_list()
        return platform_na_sales_more_mln_top
    
    @task()
    def get_top_publisher_mean_jp_sales(df):
        df = pd.read_csv(StringIO(df))
        publisher_mean_jp_sales = df.groupby('Publisher').JP_Sales.mean().sort_values(ascending=False)
        publisher_mean_jp_sales_top = publisher_mean_jp_sales[publisher_mean_jp_sales == 
                                                      publisher_mean_jp_sales.max()].index.to_list()
        return publisher_mean_jp_sales_top
    
    @task()
    def get_count_games_eu_better_jp(df):
        df = pd.read_csv(StringIO(df))
        games_eu_better_jp = str(len(df[df.EU_Sales > df.JP_Sales]))
        return games_eu_better_jp
    
    @task(on_success_callback=send_message)
    def print_data(the_top_game_world, 
                   genre_eu_top, 
                   platform_na_sales_more_mln_top, 
                   publisher_mean_jp_sales_top, 
                   games_eu_better_jp):
        
        context = get_current_context()
        date = context['ds']

        the_top_game_world = the_top_game_world
        genre_eu_top = ", ".join(genre_eu_top)
        platform_na_sales_more_mln_top = ", ".join(platform_na_sales_more_mln_top)
        publisher_mean_jp_sales_top = ", ".join(publisher_mean_jp_sales_top)
        games_eu_better_jp = games_eu_better_jp

        print(f'Самая продаваемая игра во всем мире в {my_year} — {the_top_game_world}.')
        print(f'Самыми продаваевыми играми в Европе в {my_year} были игры жанра/ов {genre_eu_top}.')
        print(f'''Больше всего игр, продавшихся более чем миллионным тиражом в Северной Америке, в {my_year} было выпущено на платформе/ах — {platform_na_sales_more_mln_top}.''')
        print(f'''Издатель/и с самыми высокими средними продажами в Японии в {my_year} — {publisher_mean_jp_sales_top}.''')
        print(f'''В {my_year} в Европе продавались лучше, чем в Японии {games_eu_better_jp} игр.''')


    df = get_data()
    the_top_game_world = get_the_top_game_world(df)
    genre_eu_top = get_genre_eu_top(df)
    platform_na_sales_more_mln_top = get_top_platform_na_sales(df)
    publisher_mean_jp_sales_top = get_top_publisher_mean_jp_sales(df)
    games_eu_better_jp = get_count_games_eu_better_jp(df)

    print_data(the_top_game_world, 
                   genre_eu_top, 
                   platform_na_sales_more_mln_top, 
                   publisher_mean_jp_sales_top, 
                   games_eu_better_jp)

imamagzam_dag_lesson_3 = imamagzam_dag_lesson_3()

