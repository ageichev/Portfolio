import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 'a.nakrap',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 2),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 1319018959
try:
    BOT_TOKEN = '5278277210:AAGqCs5eWVT0ogxtVYopZYy1pIvFhVRwgO8'
except:
    BOT_TOKEN = ''

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
def nakrap_lesson_3():
    @task()
    def get_data():
        path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        df = pd.read_csv(path)
        year = 1994 + hash(f'a-nakrap-22') % 23
        df = df.query('Year == @year')
        return df

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_game(df):
        global_game = df.groupby('Name', as_index =False).Global_Sales.sum()
        top_game = global_game[global_game.Global_Sales == global_game.Global_Sales.max()]
        return top_game.to_csv(index=False)

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_top_genre_EU(df):
        genre_EU = df.groupby('Genre', as_index =False).EU_Sales.sum()
        top_genre_EU = genre_EU[genre_EU.EU_Sales == genre_EU.EU_Sales.max()]
        return top_genre_EU.to_csv(index=False)

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def get_top_platform_NA(df):
        platform = df[df.NA_Sales > 1].groupby('Platform', as_index=False).Name.count()
        platform_top = platform[platform.Name == platform.Name.max()]
        return platform_top.to_csv(index=False)

    # У какого издателя самые высокие средние продажи в Японии?
    @task()
    def get_top_publisher_JP(df):
        publisher_JP = df.groupby('Publisher', as_index=False).JP_Sales.mean()
        publisher_JP_top = publisher_JP[publisher_JP.JP_Sales == publisher_JP.JP_Sales.max()]
        return publisher_JP_top.to_csv(index=False)

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_game_EU_JP(df):
        cnt_game = (df.EU_Sales > df.JP_Sales).sum()
        return cnt_game
    
    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre_EU, platform_top, publisher_JP_top, cnt_game):

        context = get_current_context()
        date = context['ds']

        print(f'Top game by Global Sales for {date}')
        print(top_game)
        
        print(f'Top genre by EU Sales for {date}')
        print(top_genre_EU)
        
        print(f'Top platform by NA Sales for {date}')
        print(platform_top)
        
        print(f'Top publisher by JP Sales for {date}')
        print(publisher_JP_top)
        
        print(f'Count of EU Sales more than JP Sales for {date}')
        print(cnt_game)
        

    df = get_data()
    top_game = get_top_game(df)
    top_genre_EU = get_top_genre_EU(df)
    platform_top = get_top_platform_NA(df)
    publisher_JP_top = get_top_publisher_JP(df)
    cnt_game = get_game_EU_JP(df)

    print_data(top_game, top_genre_EU, platform_top, publisher_JP_top, cnt_game)

nakrap_lesson_3 = nakrap_lesson_3()
