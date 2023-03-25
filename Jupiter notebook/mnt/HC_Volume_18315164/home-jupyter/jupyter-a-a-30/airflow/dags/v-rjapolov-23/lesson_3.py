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

df = 'vgsales.csv'
login = 'ryapvlad@yandex.ru'
year = 1994 + hash(f'{login}') % 24

default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -620798068
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
def lesson_3_rva():
    @task(retries=3)
    def get_data():
        sales_data = pd.read_csv(df)
        sales_data = sales_data.query('Year == @year')
        return sales_data

    @task(retries=4, retry_delay=timedelta(10))
    def get_top_game(sales_data):
        top_game = sales_data.Name.loc[sales_data.Global_Sales.idxmax()]
        return top_game

    @task()
    def get_top_5_genre_EU(sales_data):
        top_5_genre_EU = sales_data \
                                    .groupby('Genre', as_index=False) \
                                    .agg({'EU_Sales': 'sum'}) \
                                    .sort_values('EU_Sales', ascending=False).head(5)
        return top_5_genre_EU.to_csv(index=False)

    @task()
    def get_NA_top_platform(sales_data):
        NA_top_platform = list(sales_data.query('NA_Sales >= 1').Platform.unique())
        
        return NA_top_platform

    @task()
    def get_top_5_japain_publishers(sales_data):
        top_5_japain_publishers = sales_data.groupby('Publisher', as_index=False) \
                                    .agg({'JP_Sales': 'mean'}) \
                                    .sort_values('JP_Sales', ascending=False).head(5)
        return top_5_japain_publishers.to_csv(index=False)

    @task()
    def get_games_eu_better_than_jp(sales_data):
        games_eu_better_than_jp = sales_data.query('EU_Sales - JP_Sales > 0').Name.nunique()
        return games_eu_better_than_jp
    
    
    
    @task(on_success_callback=send_message)
    def print_data(year, top_game, top_5_genre_EU, NA_top_platform, top_5_japain_publishers, games_eu_better_than_jp):

        login = 'ryapvlad@yandex.ru'
        year = 1994 + hash(f'{login}') % 24

        print(f'''Data for year {year}
                  Top game: {top_game}
                  Top 5 genres in Europe: {top_5_genre_EU}
                  Top platform in North America: {NA_top_platform}
                  Top 5 publishers in Japain: {top_5_japain_publishers}
                  Number of games that sold better in Europe than in Japan: {games_eu_better_than_jp}''')

        

    games_data = get_data()
    top_game = get_top_game(games_data)
    top_5_genre_EU = get_top_5_genre_EU(games_data)
    NA_top_platform = get_NA_top_platform(games_data)
    top_5_japain_publishers = get_top_5_japain_publishers(games_data)
    games_eu_better_than_jp = get_games_eu_better_than_jp(games_data)
    

    print_data(year, top_game, top_5_genre_EU, NA_top_platform, top_5_japain_publishers, games_eu_better_than_jp)

lesson_3_rva = lesson_3_rva()