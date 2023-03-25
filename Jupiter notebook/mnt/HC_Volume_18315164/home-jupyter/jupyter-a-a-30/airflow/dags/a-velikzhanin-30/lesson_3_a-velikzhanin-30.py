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
    'owner': 'a-velikzhanin-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 15),
    'schedule_interval': '0 11 * * *'
}

# CHAT_ID = -620798068
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass

@dag(default_args=default_args, catchup=False)
def a_velikzhanin_30_l2():
    @task(retries=3)
    def get_data():
        data = pd.read_csv('vgsales.csv')
        return data
    
    @task()
    def get_year(login):
        year = 1994 + hash(f'{login}') % 23
        return year
    
    @task
    def get_data_year(data, year):
        data = data.query('Year == @year')
        return data
    #1
    @task()
    def get_top_sold_game(data):
        top_sold_game = data.groupby("Name", as_index = True) \
                            .agg({'Global_Sales':'sum'}) \
                            .idxmax()[0]
        return top_sold_game
    #2
    @task()
    def get_genre_eu(data):
        genre_eu = data.groupby('Genre', as_index = True) \
                        .agg({'EU_Sales':'sum'}) \
                        .sort_values('EU_Sales', ascending = False) \
                        .idxmax()[0]
        return genre_eu
    #3
    @task()
    def get_platform_na(data):
        platform_na = data.query('NA_Sales >= 1') \
                            .groupby('Platform', as_index = True) \
                            .agg({'Name':'nunique'}) \
                            .sort_values('Name', ascending = False) \
                            .idxmax()[0]
        return platform_na
    #4
    @task()
    def get_jp_publisher(data):
        jp_publisher = data.groupby('Publisher', as_index = True) \
                            .agg({'JP_Sales':'mean'}) \
                            .sort_values('JP_Sales', ascending = False) \
                            .idxmax()[0]
        return jp_publisher
    #5
    @task()
    def get_games_eu_jp(data):
        games_eu_jp = data.query('EU_Sales > JP_Sales')['Name'].nunique()
        return games_eu_jp
    
    
    #print
#     @task(on_success_callback=send_message)
    @task()
    def print_data(login, year, top_sold_game, genre_eu, platform_na, jp_publisher, games_eu_jp):

#         context = get_current_context()
        print(f'''#----------------------------------------------------------#''')
    
        print(f'''Dag creator is {login}''')
        print(f'''Year used fo task is  {year}''')
        
        #1
        print(f'''Top sold game of the year is {top_sold_game}''')
        
        #2
        print(f'''Top sold genre in Europe region is {genre_eu}''')
        
        #3
        print(f'''Top platform by sales with over 1M copies sold in NA region is {platform_na}''')
        
        #4
        print(f'''Top publisher with the most avg sales in Japan is {jp_publisher}''')
        
        #5
        print(f'''Number of games which have sold better in Europe than in Japan is {genre_eu}''')

        print(f'''#----------------------------------------------------------#''')
    
    
    login = "a-velikzhanin-30"
    
    data = get_data()
    year = get_year(login)
    data = get_data_year(data)

    top_sold_game = get_top_sold_game(data)
    genre_eu = get_genre_eu(data)
    platform_na = get_platform_na(data)
    jp_publisher = get_jp_publisher(data)
    
    
    print_data(login, year, top_sold_game, genre_eu, platform_na, jp_publisher, games_eu_jp)


a_velikzhanin_30_l2 = a_velikzhanin_30_l2()
