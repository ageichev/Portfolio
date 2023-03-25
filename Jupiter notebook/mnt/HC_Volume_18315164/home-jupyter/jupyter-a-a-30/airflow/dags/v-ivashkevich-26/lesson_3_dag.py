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


default_args = {
    'owner': 'v.ivashevich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 12 * * *'
}

login = 'v-ivashkevich-26'
file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
my_year = 1994 + hash(f'{login}') % 23

CHAT_ID = 331389683
BOT_TOKEN = '5306596321:AAHy2Y5j1UjlVhIdA9j_logIalHLd7FVhw4'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


@dag(default_args=default_args, catchup=False)
def v_ivashkevich_dag_lesson3():
    @task(retries=3)
    def get_data():
        sort_games = pd.read_csv(file)
        sort_games = sort_games.rename(columns = (lambda x : x.lower()))
        sort_games = sort_games[sort_games.year == my_year]
        return sort_games
        
    @task()
    def top_year_game(sort_games):
        top_year_game = sort_games.query('global_sales == global_sales.max()').name.to_list()[0]
        return top_year_game
    
    @task()
    def top_platform(sort_games):
        top_platform = sort_games.query('na_sales >= 1')\
                 .groupby('platform', as_index = False)\
                 .agg({'name' : 'count'})\
                 .sort_values('name', ascending = False)\
                 .head(1)\
                 .platform.to_list()[0]
        return top_platform
    
    @task()
    def top_genre(sort_games):
        top_genre = sort_games.groupby('genre', as_index = False)\
              .agg({'eu_sales' : 'sum'})\
              .sort_values('eu_sales', ascending = False)\
              .head(1)\
              .genre\
              .to_list()[0]
        return top_genre
    
    @task()
    def top_mean_jp_publisher(sort_games):
        top_mean_jp_publisher = sort_games.groupby('publisher', as_index = False).agg({'jp_sales' : 'mean'}).max()[0]
        return top_mean_jp_publisher
    
    @task()
    def amount_better_eu_games(sort_games):
        amount_better_eu_games = sort_games.query('jp_sales < eu_sales').platform.count()
        return amount_better_eu_games
    
    @task(on_success_callback=send_message)
    def print_data(top_year_game, top_platform, top_genre, top_mean_jp_publisher, amount_better_eu_games ):

        context = get_current_context()
        date = context['ds']
        
        print(f'В {my_year} году:')
        print(f'Игра года  - {top_year_game}')
        print(f'Лучшая игровая платформа  - {top_platform}')
        print(f'Лучший жанр - {top_genre}')
        print(f'Лучший издатель в Японии - {top_mean_jp_publisher}')
        print(f'{amount_better_eu_games} игр продалось лучше в Европе, чем в Японии')
        
    sort_games = get_data()
    top_year_game = top_year_game(sort_games)
    top_platform = top_platform(sort_games)
    top_genre = top_genre(sort_games)
    top_mean_jp_publisher = top_mean_jp_publisher(sort_games)
    amount_better_eu_games = amount_better_eu_games(sort_games)
    print_data(top_year_game, top_platform, top_genre, top_mean_jp_publisher, amount_better_eu_games )

v_ivashkevich_dag_lesson3  =  v_ivashkevich_dag_lesson3()

        