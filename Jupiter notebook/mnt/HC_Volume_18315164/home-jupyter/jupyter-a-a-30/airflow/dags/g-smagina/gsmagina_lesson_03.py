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
from airflow.models import Variable

path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'g-smagina'
year  = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'g-smagina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 25)
}


@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)

def task_smagina():
    
    @task()
    def get_data():
        df = pd.read_csv(path_to_file)
        return df
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_game(df):
        top_game = df.query('Year == @year')\
                        .groupby('Name')\
                        .agg(np.sum)\
                        .Global_Sales.idxmax()
        return top_game
    
    # Игры какого жанра были самыми продаваемыми в Европе?
    @task()
    def get_top_genre_eu(df):
        top_genre_eu = df.query('Year == @year')\
                            .groupby('Genre')\
                            .agg({'EU_Sales': 'sum'})\
                            .EU_Sales.idxmax()
        return top_genre_eu

    # На какой платформе было больше всего игр, которые продались 
    # более чем миллионным тиражом в Северной Америке? 
    @task()
    def get_top_platform_na(df):
        top_platform_na =  df.query('Year == @year & NA_Sales > 1')\
                                .groupby('Platform')\
                                .agg({'NA_Sales': 'sum'})\
                                .NA_Sales.idxmax()
        return top_platform_na

    # У какого издателя самые высокие средние продажи в Японии? 
    @task()
    def get_top_publisher_jp(df):
        top_publisher_jp = df.query('Year == @year')\
                                .groupby('Publisher')\
                                .agg({'JP_Sales': 'mean'})\
                                .JP_Sales.idxmax()
        return top_publisher_jp

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_eu_jp_games(df):
        eu_jp_games = (df.query("Year == @year").EU_Sales > df.query("Year == @year").JP_Sales).sum()
        return eu_jp_games
    
    @task()
    def print_data(top_game, top_genre_eu, top_platform_na, top_publisher_jp, eu_jp_games):
        print(f'''
            Top sales game in {year}: {top_game}
            Top genre in EU in {year}: {top_genre_eu}
            Top platform in North America in {year}: {top_platform_na}
            Top publisher in Japan in {year}: {top_publisher_jp}
            Count of games in EU more than in JP in {year}: {eu_jp_games}''')

    df = get_data()
    top_game = get_top_game(df)
    top_genre_eu = get_top_genre_eu(df)
    top_platform_na = get_top_platform_na(df)
    top_publisher_jp = get_top_publisher_jp(df)
    eu_jp_games = get_eu_jp_games(df)

    print_data(top_game, top_genre_eu, top_platform_na, top_publisher_jp, eu_jp_games)

        
task_smagina = task_smagina()

