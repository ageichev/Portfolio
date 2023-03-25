import requests
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
    
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
  
year = 1994 + hash(f'o-terehina-23') % 23
    
CHAT_ID = 902684386
try:
    BOT_TOKEN = '5442233320:AAEtnqMhEA1uQnc7-yfvfDKKBgGc_BU5hAo'
except:
    BOT_TOKEN = ''
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, message=message)
    else:
        pass

default_args = {
'owner': 'o-terehina-23',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 9, 5),
'schedule_interval': '0 14 * * *'
}

@dag(default_args=default_args, catchup=False)
def top_games_o_terehina_23():
    @task()
    def get_data():
        games_data = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        return games_data
    
    
    @task()
    def top_sale_game(games_data):
        top_sale_game_data = games_data.query("Year == year")\
                                       .groupby('Name', as_index=False)\
                                       .agg({'Global_Sales':'sum'})\
                                       .sort_values('Global_Sales',ascending=False)\
                                       .head(1)
        return top_sale_game_data
    
    @task()
    def top_sale_genre_Europe(games_data):
        top_sale_genre_data = games_data.query("Year == year")\
                                        .groupby('Genre',as_index=False)\
                                        .agg({'EU_Sales':'sum'})\
                                        .sort_values('EU_Sales',ascending=False)
        return top_sale_genre_data.to_csv(index=False)
    
    
    @task()
    def top_sale_platform_NA(games_data):
        top_sale_platform_data = games_data.query("Year == year")\
                                           .groupby('Platform',as_index=False)\
                                           .agg({'NA_Sales':'sum'})\
                                           .sort_values('NA_Sales',ascending=False)
        return top_sale_platform_data.to_csv(index=False)
    
    @task()
    def top_sale_publisher_JP(games_data):
        top_sale_publisher_data = games_data.query("Year == year")\
                                            .groupby('Publisher',as_index=False)\
                                            .agg({'JP_Sales':'mean'})\
                                            .sort_values('JP_Sales',ascending=False)\
                                            .query("JP_Sales != 0")
        return top_sale_publisher_data.to_csv(index=False)
    
    @task()
    def top_sale_game_EU(games_data):
        top_sale_game_EU_data = games_data.query("Year == year")\
                                          .groupby('Name',as_index=False)\
                                          .agg({'EU_Sales':'sum',
                                                'JP_Sales':'sum'})\
                                          .sort_values(['EU_Sales','JP_Sales'],ascending=False)\
                                          .query("EU_Sales >JP_Sales")
        return top_sale_game_EU_data.to_csv(index=False)
    
    @task()
    def print_data(top_sale_game_data, top_sale_genre_data,top_sale_platform_data,
                   top_sale_publisher_data,top_sale_game_EU_data):
        print(f'Top Sale games in Global')
        print (top_sale_game_data)
    
        print(f'Top Sale genre in Europe')
        print (top_sale_genre_data)
    
        print(f'Top Sale platform in North America')
        print (top_sale_platform_data)
    
        print(f'Top Sale publisher in Japan')
        print (top_sale_publisher_data)
    
        print(f'Top Sale games in Europe')
        print (top_sale_game_EU_data)
    
    games_data = get_data()
    top_sale_game_data = top_sale_game(games_data)
    top_sale_genre_data = top_sale_genre_Europe(games_data)
    top_sale_platform_data = top_sale_platform_NA(games_data)
    top_sale_publisher_data = top_sale_publisher_JP(games_data)
    top_sale_game_EU_data = top_sale_game_EU(games_data)
    print_data(top_sale_game_data, top_sale_genre_data,top_sale_platform_data,
               top_sale_publisher_data,top_sale_game_EU_data) 
    
    top_games = top_games()
 
  