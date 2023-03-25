import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import requests
import json
from urllib.parse import urlencode

year = 1994 + hash(f'v-misjura-20') % 23

schedule_interval = '0 11 * * *'

default_args = {
    'owner': 'v_misjura_20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 27),
}

CHAT_ID = 1376065608
BOT_TOKEN = '5266191463:AAEe9S06eHmMVZ2B6cUaH9SfOGpo7hmeYPw'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Your dag {dag_id} completed on {date}'
    params = {'chat_id': CHAT_ID, 'text': message}
    base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)

@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def dag_v_misjura_lesson_3():
    @task(retries=3)
    def get_data():
        games = pd.read_csv('vgsales.csv')
        games = games[games.Year == year]
        return games

    @task()
    def get_most_popular_game(games):
        game = games.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending=False)
        game = game[game['Global_Sales']==game['Global_Sales'].max()]
        return game

    @task()
    def get_most_popular_genre_in_Europe(games):
        genre = games.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False)
        genre = genre[genre['EU_Sales']==genre['EU_Sales'].max()]
        return genre

    @task()
    def get_platform_for_games_in_NA(games):
        NA_sales = games.groupby('Name', as_index=False).agg({'NA_Sales':'sum'})
        NA_sales = NA_sales[NA_sales['NA_Sales'] > 1]
        NA_sales = pd.merge(NA_sales, games[['Name', 'Platform']], on='Name')
        platform = NA_sales.groupby('Platform', as_index=False).agg({'Name':'count'}).sort_values('Name', ascending=False)
        platform[platform['Name']==platform['Name'].max()]
        return platform
    
    @task()
    def get_publisher_in_Japan(games):
        publisher = games.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending=False)
        publisher = publisher[publisher['JP_Sales']==publisher['JP_Sales'].max()]
        return publisher
    
    @task()
    def get_sales_Europe_Japan(games):
        EU_JP_sales = games.groupby('Name', as_index=False).agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        EU_JP_sales = EU_JP_sales[EU_JP_sales['EU_Sales'] > EU_JP_sales['JP_Sales']]
        return EU_JP_sales

    @task(on_success_callback=send_message)
    def print_data(game, genre, platform, publisher, EU_JP_sales):
        context = get_current_context()
        date = context['ds']
        year = 1994 + hash(f'v-misjura-20') % 23
        most_popular_game = game['Name']
        most_popular_genre_in_Europe = genre['Genre']
        platform_for_games_in_NA = platform['Platform']
        publisher_in_Japan = publisher['Publisher']
        sales_Europe_Japan = EU_JP_sales['Name'].count()
        
        print(f'Data for date {date}:')
        print(f'Most popular game in {year} is {most_popular_game}')
        print(f'Most popular genre in Europe in {year} is {most_popular_genre_in_Europe}')
        print(f'Platform that sold the most games with over a million copies in North America in {year} is {platform_for_games_in_NA}')
        print(f'Publisher that had the highest average sales in Japan in {year} is {publisher_in_Japan}')
        print(f'{sales_Europe_Japan} games sold better in Europe than in Japan in {year}')

    
    games = get_data()
    
    game = get_most_popular_game(games)
    genre = get_most_popular_genre_in_Europe(games)
    platform = get_platform_for_games_in_NA(games)
    publisher = get_publisher_in_Japan(games)
    EU_JP_sales = get_sales_Europe_Japan(games)
    
    print_data(game, genre, platform, publisher, EU_JP_sales)
    
dag_v_misjura_lesson_3 = dag_v_misjura_lesson_3()