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

TOP_1M_DOMAINS_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
YEAR = 1994 + hash(f'f-hanipov-23') % 23

default_args = {
    'owner': 'f-hanipov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 5),
    'schedule_interval': '0 15 * * *'
}

CHAT_ID = -1910055380
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
def f_hanipov_lesson_3():
    @task(retries=5)
    def get_data_games():
        data_games = pd.read_csv(TOP_1M_DOMAINS_FILE).query('Year == @YEAR')
        return data_games

    @task()
    def get_top_game(data_games):
        top_game = data_games.Name.iloc[0]
        return {'top_game': top_game}

    @task()
    def get_genre_eu(data_games):
        best_genre_eu = data_games.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending=False).Genre.iloc[0]
        return {'best_genre_eu': best_genre_eu}
    
    @task()
    def get_plat_na(data_games):
        best_plat_na = data_games.query('NA_Sales >= 1').groupby('Platform', as_index=False) \
            .agg({'Name': 'count'}).sort_values('Name', ascending=False).Platform.iloc[0]
        return {'best_plat_na': best_plat_na}

    @task()
    def get_pub_jp(data_games):
        best_pub_jp = data_games.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).Publisher.iloc[0]
        return {'best_pub_jp': best_pub_jp}

    @task()
    def get_count_games(data_games):
        count_games = data_games.query('EU_Sales > JP_Sales').shape[0]
        return {'count_games': count_games}

    @task(on_success_callback=send_message)
    def print_data(stat2, stat3, stat4, stat5, stat6):

        context = get_current_context()
        date = context['ds']

        best_game, best_genre, best_plat = stat2['top_game'], stat3['best_genre_eu'], stat4['best_plat_na']
        best_pub, best_count_games = stat5['best_pub_jp'], stat6['count_games']
        print(f'''Data from .RU for {date}
                  Best game: {best_game}
                  Best genre in Europe: {best_genre}
                  Best platform in NA: {best_plat}
                  Best publisher in JP: {best_pub}
                  Count games EU more than JP: {best_count_games}''')

        
    data_games = get_data_games()
    stat2 = get_top_game(data_games)
    stat3 = get_genre_eu(data_games)

    stat4 = get_plat_na(data_games)
    stat5 = get_pub_jp(data_games)
    stat6 = get_count_games(data_games)

    print_data(stat2, stat3, stat4, stat5, stat6)

f_hanipov_lesson_3 = f_hanipov_lesson_3()