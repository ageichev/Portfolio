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

TOP_1M_DOMAINS_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
YEAR = 1994 + hash(f'o-kivokurtsev') % 23

default_args = {
    'owner': 'o.kivokurtsev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 1),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def o_kivokurtsev_lesson_3():
    @task(retries=3)
    def get_data_games():
        games = pd.read_csv(TOP_1M_DOMAINS_FILE).query('Year == @YEAR')
        return games

    @task()
    def get_top_game(games):
        top_game = games.Name.iloc[0]
        return {'top_game': top_game}

    @task()
    def get_genre_eu(games):
        best_genre_eu = games.groupby('Genre', as_index=False)             .agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending=False).Genre.iloc[0]
        return {'best_genre_eu': best_genre_eu}
    
    @task()
    def get_plat_na(games):
        best_plat_na = games.query('NA_Sales >= 1').groupby('Platform', as_index=False)             .agg({'Name': 'count'}).sort_values('Name', ascending=False).Platform.iloc[0]
        return {'best_plat_na': best_plat_na}

    @task()
    def get_pub_jp(games):
        best_pub_jp = games.groupby('Publisher', as_index=False)             .agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).Publisher.iloc[0]
        return {'best_pub_jp': best_pub_jp}

    @task()
    def get_count_games(games):
        count_games = games.query('EU_Sales > JP_Sales').shape[0]
        return {'count_games': count_games}

    @task()
    def print_data(stat2, stat3, stat4, stat5, stat6):

        context = get_current_context()
        date = context['ds']

        best_game, best_genre, best_plat = stat2['top_game'], stat3['best_genre_eu'], stat4['best_plat_na']
        best_pub, best_count_games = stat5['best_pub_jp'], stat6['count_games']
        print(f'''Data from .RU for {date}
                  Best game: {best_game}
                  Best genre in Europe: {best_genre}
                  Best platform in NA: {best_plat}
                  Best publisher in JP: {best_pub}
                  Count games EU more than JP: {best_count_games}''')

    games = get_data_games()
    stat2 = get_top_game(games)
    stat3 = get_genre_eu(games)

    stat4 = get_plat_na(games)
    stat5 = get_pub_jp(games)
    stat6 = get_count_games(games)

    print_data(stat2, stat3, stat4, stat5, stat6)

o_kivokurtev_lesson_3 = o_kivokurtsev_lesson_3()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




