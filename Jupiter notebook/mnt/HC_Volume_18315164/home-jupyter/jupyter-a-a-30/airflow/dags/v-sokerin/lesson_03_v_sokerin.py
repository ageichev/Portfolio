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

data_set = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'v-sokerin') % 23

CHAT_ID = ''
BOT_TOKEN = ''

default_args = {
    'owner': 'v.sokerin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=7),
    'start_date': datetime(2022, 10, 17),
}

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, message=message)
    else:
        pass
    
@dag(default_args=default_args, catchup=False)
def dag_v_sokerin_new():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(data_set).query('Year == @year')
        return df.to_csv(index=False)

    @task()
    def get_world_game(df):
        df = pd.read_csv(StringIO(df))
        world_game = df.groupby('Name') \
                       .agg({'Global_Sales': 'sum'}) \
                       .query('Global_Sales == Global_Sales.max()') \
                       .index.to_list()
        return world_game
    
    @task()
    def get_genre_eu(df):
        df = pd.read_csv(StringIO(df))
        genre_eu = df.groupby('Genre') \
                     .agg({'EU_Sales': 'sum'}) \
                     .query('EU_Sales == EU_Sales.max()') \
                     .index.to_list()
        return genre_eu

    @task()
    def get_platform_na(df):
        df = pd.read_csv(StringIO(df))
        platform_na = df[df.NA_Sales > 1].groupby('Platform') \
                                         .agg({'Name': 'count'}) \
                                         .query('Name == Name.max()') \
                                         .index.to_list()
        return platform_na
    
    @task()
    def get_publisher_mean_jp(df):
        df = pd.read_csv(StringIO(df))
        publisher_mean_jp = df.groupby('Publisher') \
                              .agg({'JP_Sales': 'mean'}) \
                              .query('JP_Sales == JP_Sales.max()') \
                              .index.to_list()
        return publisher_mean_jp
    
    @task()
    def get_games_eu_jp(df):
        df = pd.read_csv(StringIO(df))
        games_eu_jp = str(len(df[df.EU_Sales > df.JP_Sales]))
        return games_eu_jp
    
    @task(on_success_callback=send_message)
    def print_data(world_game, 
                   genre_eu, 
                   platform_na, 
                   publisher_mean_jp, 
                   games_eu_jp):
        
        context = get_current_context()
        date = context['ds']

        print(f'''{date} - Наиболее продаваемая игра в {year} во всем мире:
                  {", ".join(world_game)}.''')
        print(f'''{date} - Наиболее популярный жанр игр в {year} в Европе:
                  {", ".join(genre_eu)}.''')
        print(f'''{date} - Наиболее популярная игровая платформа в {year} в Северной Америке:
                  {", ".join(platform_na)}.''')
        print(f'''{date} - Издатель с самым высоким средним уровнем продаж в {year} в Японии:
                  {", ".join(publisher_mean_jp)}.''')
        print(f'''{date} - Количество игр проданых в Европе лучше, чем в Японии в {year}:
                  {games_eu_jp}.''')


    df = get_data()
    world_game = get_world_game(df)
    genre_eu = get_genre_eu(df)
    platform_na = get_platform_na(df)
    publisher_mean_jp = get_publisher_mean_jp(df)
    games_eu_jp = get_games_eu_jp(df)

    print_data(world_game, 
               genre_eu, 
               platform_na, 
               publisher_mean_jp, 
               games_eu_jp)

dag_v_sokerin_new = dag_v_sokerin_new()
