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

# Указание пути к данным
GAME_DATA_PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

# Вычисление анализируемого года (2012)
year_observed = 1994 + hash(f'r-kalinin') % 23

default_args = {
    'owner': 'r-kalinin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 15),
    'schedule_interval': '45 2 * * *'
}

# CHAT_ID = -620798068
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''
#
# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass


@dag(default_args=default_args, catchup=False)
def game_data_handling():
    @task()
    def get_data():
        games = pd.read_csv(GAME_DATA_PATH)
        games_2012 = games.query('Year == @year_observed')
        return games_2012.to_csv(index=False)

    @task()
    def get_top_game(games_2012):
        top_game_df = pd.read_csv(StringIO(games_2012))
        top_game = top_game_df.groupby('Name', as_index=False).agg({'Rank': 'count'}).sort_values('Rank', ascending= False).head(1).iloc[0]['Name']
        return top_game

    @task()
    def get_top_genre(games_2012):
        top_genre_df = pd.read_csv(StringIO(games_2012))
        top_genre = top_genre_df.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        top_genre_max_eu_sales = top_genre.EU_Sales.max()
        top_genre = top_genre.query('EU_Sales == @top_genre_max_eu_sales').Genre
        return top_genre.to_csv(index=False)

    @task()
    def get_top_platform(games_2012):
        top_platform_df = pd.read_csv(StringIO(games_2012))
        top_platform_na = top_platform_df.query('NA_Sales > 1.0').groupby('Platform', as_index=False).agg({'NA_Sales': 'sum'})
        top_platform_na_max = top_platform_na.NA_Sales.max()
        top_platform_na = top_platform_na.query('NA_Sales == @top_platform_na_max').Platform
        return top_platform_na.to_csv(index=False)

    @task()
    def get_top_publisher(games_2012):
        top_publisher_df = pd.read_csv(StringIO(games_2012))
        publisher_jp = top_publisher_df.groupby('Publisher', as_index= False).agg({'JP_Sales': 'mean'})
        publisher_jp_mean_max = publisher_jp.JP_Sales.max()
        publisher_jp = publisher_jp.query('JP_Sales == @publisher_jp_mean_max').Publisher
        return publisher_jp.to_csv(index=False)

    @task()
    def get_eu_gt_jp(games_2012):
        eu_gt_jp_df = pd.read_csv(StringIO(games_2012))
        eu_gt_jp = eu_gt_jp_df.groupby('Name', as_index=False).agg({'EU_Sales':'sum', 'JP_Sales':'sum'}).query('EU_Sales > JP_Sales').shape[0]
        return eu_gt_jp

    # @task(on_success_callback=send_message)
    @task()
    def print_data(year, game, genre, platform, publisher, eu_jp):

        print(f'Top game for {year}')
        print(game)
        print()

        print(f'Top genre in EU for {year}')
        print(genre)
        print()

        print(f'Top platform in NA for {year}')
        print(platform)
        print()

        print(f'Top publisher in JP for {year}')
        print(publisher)
        print()

        print(f'Number of games sold better in EU than JP for {year}')
        print(eu_jp)
        print()

    games_2012 = get_data()
    top_game = get_top_game(games_2012)
    top_genre = get_top_genre(games_2012)
    top_platform = get_top_platform(games_2012)
    top_publisher = get_top_publisher(games_2012)
    eu_gt_jp = get_eu_gt_jp(games_2012)

    print_data(year_observed, top_game, top_genre, top_platform, top_publisher, eu_gt_jp)


airflow_data_handling = game_data_handling()