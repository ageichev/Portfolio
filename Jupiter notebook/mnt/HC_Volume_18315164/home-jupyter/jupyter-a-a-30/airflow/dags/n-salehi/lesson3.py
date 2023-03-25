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


CHAT_ID = 484563522
try:
    BOT_TOKEN = '5138626172:AAEhii0hzJdLRbapNxScmM_8x05IRBCiLYA'
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


default_args = {
    'owner': 'n-salehi',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 29),
    'schedule_interval': '0 12 * * *'
}

link = 'https://drive.google.com/file/d/1dqesdMfFgoAX9gHfOmZ-sU5Rp66Hxbdr/view?usp=drivesdk'

path = 'https://drive.google.com/uc?export=download&id='+link.split('/')[-2]

YEAR = 1994 + hash(f'n-salehi') % 23

@dag(default_args=default_args, catchup=False)
def n_salehi_3():
    @task(retries=3)
    def get_data():
        data = pd.read_csv(path).query('Year == @YEAR')
        return data


    @task(retries=2)
    def get_table(data):
            top_game = data.Name.iloc[0]
            return {'top_game': top_game}


    @task()
    def get_genre_EU(data):
            top_genre = data.groupby('Genre', as_index=False) \
                .agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending=False).Genre.iloc[0]
            return {'top_genre': top_genre}


    @task(retries=2)
    def get_top_platform(data):
            top_platform_NA = data.query('NA_Sales >= 1').groupby('Platform', as_index=False) \
                .agg({'Name': 'count'}).sort_values('Name', ascending=False).Platform.iloc[0]
            return {'top_platform_NA': top_platform_NA}


    @task(retries=2)
    def get_publisher(data):
            top_publisher_JP = data.groupby('Publisher', as_index=False) \
                .agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).Publisher.iloc[0]
            return {'top_publisher_JP': top_publisher_JP}

    @task(retries=2)
    def get_diff(data):
            games_EU_more_than_JP = data.query('EU_Sales > JP_Sales').shape[0]
            return {'games_EU_more_than_JP': games_EU_more_than_JP}



    @task(on_success_callback=send_message)
    def print_data(stat2, stat3, stat4, stat5, stat6):

            context = get_current_context()
            date = context['ds']

            best_game, best_genre, best_plat = stat2['top_game'], stat3['top_genre'], stat4['top_platform_NA']
            best_pub, compare_Eu_JP = stat5['top_publisher_JP'], stat6['games_EU_more_than_JP']
            print(f'''Data from games rank for {date}
                      Best game: {best_game}
                      Best genre in Europe: {best_genre}
                      Best platform in NA: {best_plat}
                      Best publisher in JP: {best_pub}
                      Games sales EU more than JP: {compare_Eu_JP}''')


    data = get_data()
    stat2 = get_table(data)
    stat3 = get_genre_EU(data)
    stat4 = get_top_platform(data)
    stat5 = get_publisher(data)
    stat6 = get_diff(data)
    print_data(stat2, stat3, stat4, stat5, stat6)

n_salehi_3 = n_salehi_3()

