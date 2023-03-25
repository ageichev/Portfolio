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


default_args = {
    'owner': 'n-baldina-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 9)
}
schedule_interval = '0 12 * * *'

@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def nbaldina_games():
    @task(retries=3)
    def get_data():
        year = 1994 + hash(f'n-baldina-24') % 23
        games = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        games_2013 = games.query('Year == @year')
        return games_2013

    @task(retries=4, retry_delay=timedelta(10))
    def get_global_game(games_2013):
        global_game = games_2013.groupby('Name') \
                                .agg({'Global_Sales': 'sum'}) \
                                .sort_values('Global_Sales', ascending=False) \
                                .idxmax()
        return global_game

    @task()
    def get_top_5_eu_genre(games_2013):
        top_5_eu_genre = games_2013.groupby('Genre') \
                                    .agg({'EU_Sales': 'sum'}) \
                                    .sort_values('EU_Sales', ascending=False) \
                                    .head()
        return top_5_eu_genre

    @task()
    def get_platform_NA(games_2013):
        platform_NA = games_2013.query('NA_Sales>=1') \
                                .groupby('Platform', as_index=False) \
                                .agg({'Name': 'nunique'}) \
                                .sort_values('Name', ascending=False)
        platform_NA = platform_NA.Platform.to_list()
        return platform_NA

    @task()
    def get_publisher_jp(games_2013):
        publisher_jp = games_2013.groupby('Publisher') \
                                .agg({'JP_Sales': 'mean'}) \
                                .idxmax()
        return publisher_jp
    
    @task()
    def get_eu_over_jp(games_2013):
        tab_eu = games_2013.groupby('Name', as_index=False) \
                            .agg({'EU_Sales': 'sum'})
        tab_jp = games_2013.groupby('Name', as_index=False) \
                            .agg({'JP_Sales': 'sum'})
        tab = tab_eu.merge(tab_jp, on='Name', how='inner')
        tab['diff'] = tab['EU_Sales'] - tab['JP_Sales']
        eu_over_jp = tab.query('diff>0').Name.nunique()
        return eu_over_jp

    @task()
    def print_data(global_game, top_5_eu_genre, platform_NA, publisher_jp, eu_over_jp):

        print('Какая игра была самой продаваемой в этом году во всем мире?')
        print(global_game)
        print('Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько')
        print(top_5_eu_genre)
        print('На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько')
        print(platform_NA)
        print('У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько')
        print(publisher_jp)
        print('Сколько игр продались лучше в Европе, чем в Японии?')
        print(eu_over_jp)

    games_2013 = get_data()
    global_game = get_global_game(games_2013)
    top_5_eu_genre = get_top_5_eu_genre(games_2013)
    platform_NA = get_platform_NA(games_2013)
    publisher_jp = get_publisher_jp(games_2013)
    eu_over_jp = get_eu_over_jp(games_2013)

    print_data(global_game, top_5_eu_genre, platform_NA, publisher_jp, eu_over_jp)

nbaldina_games = nbaldina_games()