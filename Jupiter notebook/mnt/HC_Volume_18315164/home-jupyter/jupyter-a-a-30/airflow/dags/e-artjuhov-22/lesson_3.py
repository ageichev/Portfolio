import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram
import json
from urllib.parse import urlencode

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'e-artjuhov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 12, 13),
    'schedule_interval': '0 12 * * *'
}

year = 1994 + hash(f'e-artjuhov-22') % 23

CHAT_ID = 342740720
try:
    BOT_TOKEN = '5289111807:AAFOzNB3KxTGZDr9w1oCeTJ8m15SgFNlD9M'
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
def e_artjuhov_22_lesson_3():
    @task()
    def get_data():
        df = pd.read_csv(vgsales).query("Year == @year")
        return df
    
    @task()
    def get_best_seller(df):
        return df.groupby('Name').sum().Global_Sales.idxmax()
    
    @task()
    def get_genre_eu(df):
        return df.groupby('Genre').sum().EU_Sales.idxmax()
    
    @task()
    def get_platform_na(df):
        return df.query('NA_Sales > 1').groupby('Platform').sum().NA_Sales.idxmax()
    
    @task()
    def get_publisher_jp(df):
        return df.groupby('Publisher').mean().JP_Sales.idxmax()
    
    @task()
    def get_games_eu_better_jp(df):
        return df.query('EU_Sales > JP_Sales').shape[0]
    
    @task(on_success_callback=send_message)
    def print_data(top_game, genre_eu, platform_na, publisher_jp, games_eu_better_jp):
        
        context = get_current_context()
        date=context['ds']
        print(f'''
            The best selling game worldwide in {year}: {top_game}
            Top genre in EU in {year}: {genre_eu}
            Top platform in NA in {year}: {platform_na}
            Top publisher in Japan in {year}: {publisher_jp}
            The number of games sold in Europe is better than in Japan in {year}: {games_eu_better_jp}'''             
            )
    df = get_data()
    
    top_game = get_best_seller(df)
    genre_eu = get_genre_eu(df)
    platform_na = get_platform_na(df)
    publisher_jp = get_publisher_jp(df)
    games_eu_better_jp = get_games_eu_better_jp(df)
    
    print_data(top_game, genre_eu, platform_na, publisher_jp, games_eu_better_jp)
    
e_artjuhov_22_lesson_3 = e_artjuhov_22_lesson_3()