import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 'a.holodkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 17),
    'schedule_interval': '0 12 * * *'}

CHAT_ID = -774839853
BOT_TOKEN = Variable.get('telegram_secret')


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f"Даг {dag_id} завершён {date}."
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

 
@dag(default_args=default_args, catchup=False)
def lesson_03_aholodkov_20():
    
    @task()
    def get_data():
        year = 1994 + hash(f'{"a-holodkov-20"}') % 23
        games_data = pd.read_csv("vgsales.csv").query("Year == @year")
        return games_data   
    
    
    #Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def game_top_1(games_data):
        top_1_game = games_data.groupby("Name",as_index=False)\
                               .agg({"Global_Sales":"sum"})\
                               .sort_values("Global_Sales",ascending=False).head(1).Name
        return top_1_game
    
    
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def games_top_genre_EU(games_data):
        top_genre_EU = games_data.groupby("Genre",as_index=False)\
                                 .agg({"EU_Sales":"sum"})\
                                 .sort_values("EU_Sales",ascending=False).head(1).Genre
        return top_genre_EU
    
    
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def games_top_platform_NA(games_data):
        top_platform_NA = games_data.query("NA_Sales > 1")\
                                    .groupby("Platform",as_index = False)\
                                    .agg({"Name":pd.Series.nunique})\
                                    .sort_values("Name",ascending=False).head(2).Platform
        return top_platform_NA
    
    
    #У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def avg_top_publisher_JP(games_data):
        top_publisher_JP = games_data.groupby("Publisher",as_index = False)\
                                     .agg({"JP_Sales":"mean"})\
                                     .sort_values("JP_Sales",ascending=False).head(2).Publisher
        return top_publisher_JP
    
    
    #Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def games_EU_more_JP(games_data):
        count_EU_more_JP = games_data.groupby("Name",as_index = False)\
                                     .agg({"EU_Sales":"sum","JP_Sales":"sum"})\
                                     .query("EU_Sales > JP_Sales")\
                                     .shape[0]
        return count_EU_more_JP

    
    @task(on_success_callback=send_message)
    def print_data(top_1_game, top_genre_EU, top_platform_NA, top_publisher_JP, count_EU_more_JP):
        context = get_current_context()
        date = context['ds']
        year = 1994 + hash('a-holodkov-20') % 23

        print(f"Самая продаваемой игрой за {year} год, была {top_1_game}")
        print(f"Самые продаваемые жанры в Европе за {year} год это - {top_genre_EU}")
        print(f"Самые популярная платформа с более чем миллионым тиражом в Северной америке за {year} год, была {top_platform_NA}")
        print(f"Самые высокие средние продажи в Японии за {year} год , у {top_publisher_JP}")
        print(f"{count_EU_more_JP} игр продались лучше в Европе чем в Японии за {year}")
        

    data = get_data()
    top_world_game = game_top_1(data)
    top_EU_genre = games_top_genre_EU(data)
    platform_1mln_NA = games_top_platform_NA(data)
    top_1_publisher_JP  = avg_top_publisher_JP(data)
    count_EU_than_JP  = games_EU_more_JP(data)
    print_data(top_world_game, top_EU_genre, platform_1mln_NA, top_1_publisher_JP, count_EU_than_JP)
    
lesson_03_aholodkov_20 = lesson_03_aholodkov_20()
        
