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


site_path =  'https://git.lab.karpov.courses/lab/airflow/-/blob/master/dags/a.batalov/vgsales.csv'
games_path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
find_year = 1994 + hash(f'{"d-kuznetsov-25"}') % 23


default_args = {
    'owner': 'd-kuznetsov-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 19),
    'schedule_interval': '0 12 * * *'
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
def Kuznetsov_25_airflow_2():
    @task()
    def get_data():
        
        games_data = pd.read_csv(games_path)
                
        return games_data             
       

    @task()
    def get_most_played_game(games_data):
        games_info = games_data
        most_played_game = games_info.query('Year == @find_year').sort_values(by='Global_Sales', ascending=False)[['Name', 'Global_Sales']].head(1)
        return most_played_game

    @task()
    def get_top_10_EU_genre(games_data):
        games_info = games_data
        top_10_EU_genre = games_info.query('Year == @find_year').groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).sort_values(by='EU_Sales', ascending=False).head(10)
        return top_10_EU_genre

    @task()
    def get_top_10_NA_platforms(games_data):
        games_info = games_data
        top_10_NA_platforms = games_info.query('NA_Sales > 1 and Year == @find_year').groupby('Platform', as_index=False).agg({'NA_Sales': 'sum'}).sort_values(by='NA_Sales', ascending=False).head(10)
        return top_10_NA_platforms

    @task()
    def get_top_10_JP_publisher(games_data):
        games_info = games_data
        top_10_JP_publisher = games_info.query('Year == @find_year').groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).sort_values(by='JP_Sales', ascending=False).head(10)
        return top_10_JP_publisher
    
    @task()
    def get_EU_better_sales_than_JP(games_data):
        games_info = games_data
        EU_better_sales_than_JP = games_info.query('Year == @find_year').query('EU_Sales - JP_Sales > 0').Rank.count()
        return EU_better_sales_than_JP

    @task()
    def print_data(most_played_game, top_10_EU_genre, top_10_NA_platforms, top_10_JP_publisher, EU_better_sales_than_JP):

        context = get_current_context()
        date = context['ds']    

        print(f'''Most played game in world for {date}
                  is {most_played_game}''')
        
        print(f'''Top 10 EU genre for {date}
                  is {top_10_EU_genre}''')
        
        print(f'''Top 10 NA platforms for {date}
                  is {top_10_NA_platforms}''')
        
        print(f'''Top 10 JP publisher for {date}
                  is {top_10_JP_publisher}''')
        
        print(f'''EU better sales than JP for {date}
                  is {EU_better_sales_than_JP}''')

    
    games_df = get_data()
            
    print_data(get_most_played_game(games_df), get_top_10_EU_genre(games_df), get_top_10_NA_platforms(games_df), get_top_10_JP_publisher(games_df), get_EU_better_sales_than_JP(games_df))

Kuznetsov_25_airflow_2 = Kuznetsov_25_airflow_2()