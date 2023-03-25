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
    'owner': 'a.zorenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 6 * * *'
}


CHAT_ID = 61523126
    
try:
    BOT_TOKEN = Variable.get('telegram_secret')
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
def zorenko_lesson3_dag():
    @task(retries=3)
    
    def year():
        year = 1994 + hash(f'a-zorenko-20') % 23
        return year
    
    def get_data():
        #path_to_file = '/mnt/HC_Volume_18315164/home-jupyter/jupyter-a-zorenko-20/Module_11/vgsales.csv'
        year = 1994 + hash(f'a-zorenko-20') % 23
        df_pre = pd.read_csv('vgsales.csv', sep=",", low_memory=False)
        df = df_pre.query('Year == @year')
        return df

    @task()
    def top_selling_world_game(df):
        top_selling_world_game = df.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending=False).head(1).Name.to_list()[0]
        return top_selling_world_game

    @task()
    def europe_genre(df):
        europe_genre = df[['Genre', 'EU_Sales']].groupby('Genre', as_index=False).sum().sort_values('EU_Sales', ascending=False).head(1).Genre.to_list()[0]
        return europe_genre
    
    @task()
    def million_na_games(df):
        million_na_games = df[df['NA_Sales'] > 1].groupby('Platform', as_index=False).agg({'NA_Sales': 'count'}).sort_values('NA_Sales', ascending=False).head(1).Platform.to_list()[0]
        return million_na_games

    @task()
    def top_j_publisher(df):
        top_j_publisher = df.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).head(1).Publisher.to_list()[0]
        return top_j_publisher

    @task()
    def df_eu_j(df):
        df_eu_j = df.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        df_eu_j = df_eu_j[df_eu_j.JP_Sales < df_eu_j.EU_Sales].shape[0]
        return df_eu_j

    @task(on_success_callback=send_message)
    def print_data(top_selling_world_game, europe_genre, million_na_games, top_j_publisher, df_eu_j):

        context = get_current_context()
        date = context['ds']

        year = 1994 + hash(f'a-zorenko-20') % 23
        
        print(f'Best selling game in {year} - ', top_selling_world_game)
        print(f'Most popular game genre in {year} - ', europe_genre)
        print(f'Best platform with "Millon" games in NA in {year} - ', million_na_games)
        print(f'Top publisher in Japan in {year} - ', top_j_publisher)
        print(f'Count of games, selled better in Europe than in Japan in {year} - ', df_eu_j)
 
    df = get_data()
    top_selling_world_game = top_selling_world_game(df)
    europe_genre = europe_genre(df)
    million_na_games = million_na_games(df)
    top_j_publisher = top_j_publisher(df)
    df_eu_j = df_eu_j(df)
    
    print_data(top_selling_world_game, europe_genre, million_na_games, top_j_publisher, df_eu_j)

zorenko_lesson3_dag = zorenko_lesson3_dag()
