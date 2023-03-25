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
from airflow.models import Variable # 

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'a-vladimirova'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'a-vladimirova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 26),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 329568729
BOT_TOKEN = '5450212898:AAFOT94VStBauZfQfVY94mT8a0egtW3NHnU'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)    

@dag(default_args=default_args, catchup=False)
def a_vladimirova_games():
    @task(retries=3)
    def get_data_games():
        df = pd.read_csv(vgsales)
        df_games = df.query('Year==@year')
        return df_games

    @task()
    def get_the_best_selling_game(df_games):   
        the_best_selling_game = df_games.sort_values('Rank', ascending=True).iloc[0,1]
        return the_best_selling_game

    @task(retries=3)
    def get_the_best_selling_genre_eu(df_games):
        the_best_selling_genre_eu = df_games.groupby('Genre', as_index=False).agg({'EU_Sales' : 'sum'}).iloc[0,0]
        return the_best_selling_genre_eu

    @task()
    def get_platform_game_na(df_games):
        df_na_mil = df_games.query('NA_Sales >= 1')
        platform_game_na = df_na_mil.groupby('Platform', as_index=False).agg({'Name':'count'}).sort_values('Name', ascending=False).iloc[0,0]
        return platform_game_na

    @task()
    def get_publisher_jp(df_games):
        publisher_jp = df_games.groupby('Publisher', as_index=False).agg({'JP_Sales' : 'mean'}).sort_values('JP_Sales', ascending=False).iloc[0,0] 
        return publisher_jp

    @task()
    def get_eu_vs_jp(df_games):
        eu_vs_jp_games = df_games.groupby('Name', as_index=False).agg({'JP_Sales': 'sum', 'EU_Sales': 'sum'}).query('EU_Sales>JP_Sales').shape[0]
        return eu_vs_jp_games

    @task(on_success_callback=send_message)
    def print_data(the_best_selling_game, the_best_selling_genre_eu, platform_game_na, publisher_jp, eu_vs_jp_games):
        context = get_current_context()
        date = context['ds']

        print(f'''The world best selling game in {year}: {the_best_selling_game}
                      The EU best selling genre in {year}: {the_best_selling_genre_eu}
                      The best platform NA in {year}: {platform_game_na}
                      Best Japanese Publisher by Average Sales in {year}: {publisher_jp}
                      Number of games that have sold better in Europe than in Japan in {year}: {eu_vs_jp_games}''')
        
    df_games = get_data_games()
    the_best_selling_game = get_the_best_selling_game(df_games)
    the_best_selling_genre_eu = get_the_best_selling_genre_eu(df_games)
    platform_game_na = get_platform_game_na(df_games)
    publisher_jp = get_publisher_jp(df_games)
    eu_vs_jp_games = get_eu_vs_jp(df_games)

    print_data(the_best_selling_game, the_best_selling_genre_eu, platform_game_na, publisher_jp, eu_vs_jp_games)

a_vladimirova_games = a_vladimirova_games()