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
from airflow.models import Variable

FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'v-korepanov-27'
YEAR = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'v_korepanov_27',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 9),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def v_korepanov_27_l3():
    
    @task()
    def get_data():
        data = pd.read_csv(FILE)
        data = data[data['Year'] == YEAR].dropna(subset=['Year', 'Name', 'Platform', 'Genre', 'Publisher'])
        print(data.head(5))
        return data.to_csv(index=False)

    @task()
    def top_game(data):
        top_game_df = pd.read_csv(StringIO(data))
        top_game = 'Нет записей на этот год' if not list(top_game_df.groupby('Name')['Global_Sales'].sum()) else top_game_df.groupby('Name')['Global_Sales'].sum().idxmax()
        return top_game

    @task()
    def top_genre_eu(data):
        top_genre_eu_df = pd.read_csv(StringIO(data))
        top_genre_eu = top_genre_eu_df.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        top_genre_eu = ', '.join(top_genre_eu[top_genre_eu['EU_Sales'] == top_genre_eu['EU_Sales'].max()]['Genre'])
        return top_genre_eu

    @task()
    def top_platform_na(data):
        top_platform_df = pd.read_csv(StringIO(data))
        top_platform = top_platform_df.query('NA_Sales > 1').groupby('Platform', as_index=False).agg(cc=('Rank', 'count'))
        top_platform = ', '.join((list(map(str, top_platform[top_platform['cc'] == top_platform['cc'].max()]['Platform'].values))))
        return top_platform

    @task()
    def top_publisher_jp(data):
        top_publisher_jp_df = pd.read_csv(StringIO(data))
        top_publisher = top_publisher_jp_df.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'})
        top_publisher = ', '.join(top_publisher[top_publisher['JP_Sales'] == top_publisher['JP_Sales'].max()]['Publisher'])
        return top_publisher

    @task()
    def count_better_EU_JP(data):
        count_better_EU_JP_df = pd.read_csv(StringIO(data))
        count_better_EU_JP = count_better_EU_JP_df.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).query('EU_Sales > JP_Sales').shape[0]
        return count_better_EU_JP


    @task()
    def print_data(game, platform, genre, publisher, best):
        print(f'''Cамая продаваемая игра в {YEAR} году во всем мире: {game}
Игры этого жанра были самыми продаваемыми в Европе: {genre}
На этой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке: {platform}
У этого издателя самые высокие средние продажи в Японии: {publisher}
Вот столько игр продались лучше в Европе, чем в Японии: {best}''')

    data = get_data()
    game = top_game(data)
    genre = top_genre_eu(data)
    platform = top_platform_na(data)
    publisher = top_publisher_jp(data)
    best = count_better_EU_JP(data)

    print_data(game, platform, genre, publisher, best)
    
v_korepanov_27_l3 = v_korepanov_27_l3()
