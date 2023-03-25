#!/usr/bin/env python
# coding: utf-8

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

TOP_1M_DOMAINS_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
YEAR = 1994 + hash(f'e-sokolskaia-26') % 23

default_args = {
    'owner': 'e_sokolskaia',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def es26_lesson_3():
    @task(retries=2)
    def get_data_games():
        data_games = pd.read_csv(TOP_1M_DOMAINS_FILE).query('Year == @YEAR')
        return data_games

    # 1. Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_worldtop_game(data_games):
        worldtop_game = data_games.groupby('Name', as_index = False).agg({'Global_Sales': 'sum'}).sort_values(by='Global_Sales', ascending=False).head(1)
        return worldtop_game.to_csv(index=False, header=False)

    # 2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если несколько.
    @task()
    def get_top_genre_eu(data_games):
        genre_eu = data_games.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        top_genre_eu_list = genre_eu[genre_eu['EU_Sales']==genre_eu['EU_Sales'].max()]['Genre']
        return top_genre_eu_list.to_csv(index=False, header=False)
 

    # 3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    # Перечислить все, если их несколько
    @task()
    def get_platform_na(data_games):
        platform_na = data_games.query('NA_Sales >= 1').groupby('Platform', as_index=False).agg({'Name': 'count'})
        top_platform_na_list = platform_na[platform_na['Name']== platform_na['Name'].max()]['Platform']
        return top_platform_na_list.to_csv(index=False, header=False)

    
    # 4. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_publ_jp(data_games):
        publ_jp = data_games.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'})
        top_publ_jp = publ_jp[publ_jp['JP_Sales']== publ_jp['JP_Sales'].max()]['Publisher']
        return top_publ_jp.to_csv(index=False, header=False)

    # 5. Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_count_games(data_games):
        eu_jp = data_games.groupby('Name', as_index=False).agg({'JP_Sales': 'sum', 'EU_Sales': 'sum'})
        eu_more_jp_count = eu_jp.query("EU_Sales > JP_Sales").agg({'Name':'count'})
        return eu_more_jp_count.to_csv(index=False, header=False)

    @task()
    def print_data(worldtop_game, 
                   top_genre_eu_list, 
                   top_platform_na_list,
                   top_publ_jp,
                   eu_more_jp_count):
        context = get_current_context()
        date = context['ds']
        print(f"В {YEAR} году самой продаваемой игрой во всем мире была {worldtop_game}.")
        print(f"В {YEAR} году в Европе наибольшие продажи показали игры жанраов {', '.join(top_genre_eu_list)}.")
        print(f"В {YEAR} году в Северной Америке больше всего игр было продано на платформах {', '.join(top_platform_na_list)}.")
        print(f"В {YEAR} году в Японии наибольшие средние продажи игр показали издатели {', '.join(top_publ_jp)}.")
        print(f"В {YEAR} году в Европе продавались лучше, чем в Японии {eu_more_jp_count} игр.")

    data_games = get_data_games()
    worldtop_game = get_worldtop_game(data_games)
    top_genre_eu_list = get_top_genre_eu(data_games)
    top_platform_na_list = get_platform_na(data_games)
    top_publ_jp = get_publ_jp(data_games)
    eu_more_jp_count = get_count_games(data_games)
    
    print_data(worldtop_game, top_genre_eu_list, top_platform_na_list, top_publ_jp, eu_more_jp_count)
    
es26_lesson_3 = es26_lesson_3()