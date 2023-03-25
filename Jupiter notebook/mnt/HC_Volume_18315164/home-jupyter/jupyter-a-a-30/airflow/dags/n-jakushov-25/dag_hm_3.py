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
    'owner': 'n-jakushov-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 11),
    'schedule_interval': '30 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def lesson_03_njakushov_25():
    @task(retries=3)
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        login = 'n-jakushov-25'
        m_year = 1994 + hash(f'{login}') % 23
        df_m = df.query('Year == @m_year')
        return df_m

    #Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=4, retry_delay=timedelta(10))
    def get_top_game_global(df_m):
        top_game_global = df_m[df_m['Global_Sales'] == df_m['Global_Sales'].max()]['Name'].values[0]
        return top_game_global

    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_top_genre_eu(df_m):
        top_genre_eu = df_m.groupby('Genre') \
            .agg({'EU_Sales': 'sum'}) \
            .idxmax()['EU_Sales'] \
            .split(',')
        return top_genre_eu

    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def get_top_platform_na(df_m):
        top_platform_na = df_m.query('NA_Sales > 1') \
            .groupby('Platform') \
            .agg({'NA_Sales': 'sum'}) \
            .idxmax()['NA_Sales'] \
            .split(',')
        return top_platform_na

    #У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_top_publisher_jp(df_m):
        top_publisher_jp = df_m.groupby('Publisher') \
            .agg({'JP_Sales': 'mean'}) \
            .idxmax()['JP_Sales'] \
            .split(',')
        return top_publisher_jp

    #Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_games_eu_better_jp(df_m):
        games_eu_better_jp = df_m[['Name', 'EU_Sales', 'JP_Sales']] \
            .query('EU_Sales > JP_Sales') \
            ['Name'].count()
        return games_eu_better_jp

    @task()
    def print_data(top_game_global, top_genre_eu, top_platform_na, top_publisher_jp, games_eu_better_jp):
        context = get_current_context()
        date = context['ds']

        print(f'The best-selling game in the world for date {date}')
        print(top_game_global)

        print(f'The best-selling genre in Europe for date {date}')
        print(top_genre_eu)

        print(f'The best-selling platform in North America for date {date}')
        print(top_platform_na)

        print(f'The best-selling publisher in Japan for date {date}')
        print(top_publisher_jp)

        print(f'The number of games that sold better in Europe than in Japan for date {date}')
        print(games_eu_better_jp)

    df_m = get_data()
    top_game_global = get_top_game_global(df_m)
    top_genre_eu = get_top_genre_eu(df_m)
    top_platform_na = get_top_platform_na(df_m)
    top_publisher_jp = get_top_publisher_jp(df_m)
    games_eu_better_jp = get_games_eu_better_jp(df_m)
    print_data(top_game_global, top_genre_eu, top_platform_na, top_publisher_jp, games_eu_better_jp)


lesson_03_njakushov_25 = lesson_03_njakushov_25()
