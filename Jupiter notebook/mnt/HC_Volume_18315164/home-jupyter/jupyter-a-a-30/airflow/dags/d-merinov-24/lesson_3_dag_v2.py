import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
# import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

link = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'd-merinov-24'

year = float(1994 + hash(f'{login}') % 23)

default_args = {
    'owner': 'd.merinov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 10),
    'schedule_interval': '0 12 * * *'
}

# CHAT_ID = -661765706
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

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
def lesson_3_by_d_merinov():
    @task()
    def get_data():
        df = pd.read_csv('vgsales.csv')
        df_my_year = df.query('Year == @year')
        return df_my_year.to_csv(index=False)

    @task()
    def top_sales_game (df_my_year):
        df = pd.read_csv(StringIO(df_my_year))
        top_game = df.groupby('Name').agg({'Global_Sales':'sum'}).sort_values('Global_Sales',ascending=False).head(1)
        return top_game.to_csv(index=False)

    @task()
    def top_game_genre_eu (df_my_year):
        df = pd.read_csv(StringIO(df_my_year))
        top_genre_eu = df.groupby('Genre').agg({'EU_Sales':'sum'}).sort_values('EU_Sales',ascending=False).head(3)
        return top_genre_eu.to_csv(index=False)

    @task()
    def top_game_platform_na (df_my_year):
        df = pd.read_csv(StringIO(df_my_year))
        top_platform_na = df.query('NA_Sales > 1').groupby('Platform').agg({'NA_Sales':'count'}).sort_values('NA_Sales',ascending=False).head(3)
        return top_platform_na.to_csv(index=False)

    @task()
    def top_game_publisher_japan (df_my_year):
        df = pd.read_csv(StringIO(df_my_year))
        top_publisher_japan = df.groupby('Publisher').agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending=False).head(1)
        return top_publisher_japan.to_csv(index=False)

    @task()
    def top_games_eu_jp (df_my_year):
        df = pd.read_csv(StringIO(df_my_year))
        dif_eu_jp = df.EU_Sales - df.JP_Sales
        top_games_eu_jp = dif_eu_jp[dif_eu_jp > 0].count()
        return top_games_eu_jp    

    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre_eu, top_platform_na, top_publisher_japan, top_games_eu_jp):
        print(f'the best selling game in {year} worldwide is')
        print(top_game)
        
        print(f'The best-selling game genres in {year} in Europe is')
        print(top_genre_eu)
        
        print(f'The best-selling game platforms in {year} in NA is')
        print(top_platform_na)
        
        print(f'The top publisher in {year} in Japan is')
        print(top_publisher_japan)
        
        print(f'Number of games sales better EU then JP in {year} is {top_games_eu_jp}')

    top_data = get_data()
    top_sales_game = top_sales_game(top_data)
    top_game_genre_eu = top_game_genre_eu(top_data)
    top_game_platform_na = top_game_platform_na(top_data)
    top_game_publisher_japan = top_game_publisher_japan(top_data)
    top_games_eu_jp = top_games_eu_jp(top_data)

    print_data(top_sales_game, top_game_genre_eu, top_game_platform_na, top_game_publisher_japan, top_games_eu_jp)

lesson_3_by_d_merinov = lesson_3_by_d_merinov()