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
    'owner': 'v-savelev-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 12),
    'schedule_interval': '25 12 * * *'
}

CHAT_ID = 1618288919
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'All right! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def games():
    @task()
    def get_data():
        year = 1994 + hash(f'{"v-savelev-20"}') % 23
        games = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = games.query('Year == @year')
        return df

    @task()
    def get_max_global(df):
        best_game =\
            df\
                .groupby('Name', as_index=False)\
                .agg({'Global_Sales': 'sum'})\
                .sort_values('Global_Sales', ascending=False)\
                .head(10)\
                .Name\
                .to_list()[0]
        return best_game

    @task()
    def get_max_EU(df):
        max_EU =\
            df\
                .groupby('Genre', as_index=False)\
                .agg({'EU_Sales': 'sum'})\
                .EU_Sales\
                .max()

        genre_EU_max =\
            df\
                .groupby('Genre', as_index=False)\
                .agg({'EU_Sales': 'sum'})\
                .sort_values('EU_Sales', ascending=False)\
                .query('EU_Sales == @max_EU')\
                .Genre\
                .to_list()
        return genre_EU_max

    @task()
    def get_platform_NA_max(df):
        max_NA =\
            df\
                .query('NA_Sales > 1')\
                .groupby('Platform', as_index=False)\
                .agg({'Name': 'count'})\
                .rename(columns={'Name': 'Num'})\
                .Num\
                .max()

        platforms_NA =\
            df\
                .query('NA_Sales > 1')\
                .groupby('Platform', as_index=False)\
                .agg({'Name': 'count'})\
                .rename(columns={'Name': 'Num'})\
                .query('Num == @max_NA')\
                .Platform\
                .to_list()
        return platforms_NA

    @task()
    def get_max_avg_JP(df):
        max_avg_JP_sales =\
            df\
                .groupby('Publisher', as_index=False)\
                .agg({'JP_Sales': 'mean'})\
                .JP_Sales\
                .max()

        publishers_JP =\
            df\
                .groupby('Publisher', as_index=False)\
                .agg({'JP_Sales': 'mean'})\
                .query('JP_Sales == @max_avg_JP_sales')\
                .Publisher\
                .to_list()
        return publishers_JP
    
    @task()
    def get_EU_JP(df):
        num_of_games =\
            df\
                .groupby('Name', as_index=False)\
                .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})\
                .query('EU_Sales > JP_Sales')\
                .shape[0]
        return num_of_games

    @task(on_success_callback=send_message)
    def print_data(best_game, genre_EU_max, platforms_NA, publishers_JP, num_of_games):
        
        year = 1994 + hash(f'{"v-savelev-20"}') % 23
        context = get_current_context()
        date = context['ds']

        print(f'Самая продаваемая игра в мире в {year} году: {best_game}')
        print(f'Самые продаваемые жанры в Европе в {year} году: {genre_EU_max}')
        print(f'Больше всего миллионных тиражей в СА в {year} году: {platforms_NA}')
        print(f'Издатели с наибольшими средними продажами в Японии в {year} году: {publishers_JP}')
        print(f'В {year} году {num_of_games} игр продались в Европе лучше, чем в Японии')

    df = get_data()
    
    best_game = get_max_global(df)
    genre_EU_max = get_max_EU(df)
    platforms_NA = get_platform_NA_max(df)
    publishers_JP = get_max_avg_JP(df)
    num_of_games = get_EU_JP(df)

    print_data(best_game, genre_EU_max, platforms_NA, publishers_JP, num_of_games)

games = games()
