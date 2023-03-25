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

PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'd.ermolaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 12),
    'schedule_interval': '0 12 * * *'
}

login = "d-ermolaev"
year = 1994 + hash(f'{login}') % 23

@dag(default_args=default_args, catchup=False)

def games_lesson_3():
    @task(retries=3)
    def get_data_games():
        games_df = pd.read_csv(PATH)
        return games_df

    @task(retries=4, retry_delay=timedelta(10))
    def get_best_sales(games_df):
        best_sales = games_df.query('Year == @year').sort_values('Global_Sales', ascending = False).Name.head(1)
        return best_sales

    @task()
    def get_europe_genre(games_df):
        europe_genre = games_df.query('Year == @year') \
            .groupby('Genre') \
            .agg({'EU_Sales': 'mean'}) \
            .sort_values('EU_Sales', ascending=False) \
            .head(3)
        return europe_genre

    @task()
    def get_platform(games_df):
        platform = games_df.query('Year == @year & NA_Sales > 1') \
            .groupby('Platform', as_index=False) \
            .agg({'NA_Sales': 'sum'}) \
            .sort_values('NA_Sales', ascending=False) \
            .head(2)
        return platform

    @task()
    def get_publisher(games_df):
        publisher = games_df.query('Year == @year') \
            .groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values('JP_Sales', ascending=False) \
            .head(2)
        return publisher

    @task()
    def get_europe_japan(games_df):
        count_games = games_df.query('Year == @year & EU_Sales > JP_Sales').shape[0]
        return count_games


    @task()
    def print_data(best_sales,europe_genre,platform,publisher,count_games):

        context = get_current_context()
        date = context['ds']
        
        print(f'For{date}.Cамая продаваемая игра во всем мире в {year} году')
        print(best_sales)
        
        print(f'For{date}.Игры этого жанра были самыми продаваемыми в Европе в {year} году')
        print(europe_genre)
        
        print(f'For{date}.На этой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {year} году')
        print(platform)
        
        print(f'For{date}. Издатель с самыми высокими среднеми продажами в Японии в {year} году')
        print(publisher)
        
        print(f'For{date}.Количество игр, которые продались лучше в Европе, чем в Японии в {year} году')
        print(count_games)

    games_df = get_data_games()
    best_sales = get_best_sales(games_df)
    europe_genre = get_europe_genre(games_df)
    platform = get_platform(games_df)
    publisher = get_publisher(games_df)
    count_games = get_europe_japan(games_df)
    print_data(best_sales, europe_genre, platform, publisher, count_games)


games_lesson_3 = games_lesson_3()
