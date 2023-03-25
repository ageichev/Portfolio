import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import json
from urllib.parse import urlencode

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'a.sibiriakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 22),
    'schedule_interval': '45 11 * * *'
}


CHAT_ID = 344844265
try:
    BOT_TOKEN = '5789264690:AAE8L0uRRF1iiGCh-XxSKBeiltKTnvXI9AE'
except:
    BOT_TOKEN = ''   
    
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Well done! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        params = {'chat_id': CHAT_ID, 'text': message}

        base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
        url = base_url + 'sendMessage?' + urlencode(params)
        resp = requests.get(url)
    else:
        pass

VGSALES_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'    
YEAR = 1994 + hash('a-sibiriakova') % 23
    
@dag(default_args=default_args, catchup=False)
def airflow_lesson_3_a_sibiriakova():
    @task(retries=3)
    def get_year_data():
        _year_ = YEAR
        data_df = pd.read_csv(VGSALES_FILE)
        data_df_year = data_df.query('Year == @_year_')
        return data_df_year

    @task()
    def get_game_of_year(data_df_year):
        # Какая игра была самой продаваемой в этом году во всем мире?
        df_game = data_df_year.groupby('Name', as_index=False).agg({'Global_Sales':'sum'})
        
        game_of_year = str(df_game.sort_values('Global_Sales', ascending = False)['Name'].head(1).item())
        return game_of_year

    @task()
    def get_genre_eu_of_year(data_df_year):
        # Игры какого жанра были самыми продаваемыми в Европе? 
        # Перечислить все, если их несколько
        df_genre_eu = data_df_year.groupby('Genre', as_index = False) \
            .agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending = False)
        genre_eu_top = df_genre_eu.EU_Sales.max()
        df_genre_eu_top = df_genre_eu.query('EU_Sales == @genre_eu_top')
        
        return df_genre_eu_top.to_string(index=False)

    @task()
    def get_platform_na_of_year(data_df_year):
        # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
        # Перечислить все, если их несколько
        df_platform_na = data_df_year.groupby('Platform', as_index=False) \
            .agg({'NA_Sales':'sum'}).sort_values('NA_Sales', ascending = False).query('NA_Sales > 1')
        
        return df_platform_na.to_string(index=False)

    @task()
    def get_publisher_jp_of_year(data_df_year):
        # У какого издателя самые высокие средние продажи в Японии? 
        # Перечислить все, если их несколько
        df_publisher_jp = data_df_year.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending = False)
        publisher_jp_top = df_publisher_jp.JP_Sales.max()
        df_publisher_jp_top = df_publisher_jp.query('JP_Sales == @publisher_jp_top')
        
        return df_publisher_jp_top.to_string(index=False)

    @task()
    def get_eu_vs_jp_of_year(data_df_year):
        # Сколько игр продались лучше в Европе, чем в Японии?
        df_eu_vs_jp = data_df_year.groupby('Name', as_index=False).agg({'EU_Sales':'sum'}) \
            .merge(data_df_year.groupby('Name', as_index=False).agg({'JP_Sales':'sum'}), on = 'Name', how = 'left')
        
        count_of_year = str(df_eu_vs_jp.query('EU_Sales > JP_Sales').shape[0])
        return count_of_year
    
    
    @task(on_success_callback=send_message)
    def print_data(game, genre_eu, platform_na, publisher_jp, eu_vs_jp):

        print(f'===1=== Game of the year {YEAR} : {game}')
        
        print(f"===2=== Genres of the year {YEAR} in EU : {genre_eu}")
        
        print(f"===3=== Platforms of the year {YEAR} in NA : {platform_na}")
        
        print(f"===4=== Publishers of the year {YEAR} in JP : {publisher_jp}")
        
        print(f'===5=== Amount of games in the year {YEAR} EU vs JP : {eu_vs_jp}')

    data_df_year = get_year_data()

    game_of_year = get_game_of_year(data_df_year)
    genre_eu_of_year = get_genre_eu_of_year(data_df_year)
    platform_na_of_year = get_platform_na_of_year(data_df_year)
    publisher_jp_of_year = get_publisher_jp_of_year(data_df_year)
    eu_vs_jp_of_year = get_eu_vs_jp_of_year(data_df_year)

    print_data(game_of_year, genre_eu_of_year, platform_na_of_year, publisher_jp_of_year, eu_vs_jp_of_year)

airflow_lesson_3_a_sibiriakova = airflow_lesson_3_a_sibiriakova()