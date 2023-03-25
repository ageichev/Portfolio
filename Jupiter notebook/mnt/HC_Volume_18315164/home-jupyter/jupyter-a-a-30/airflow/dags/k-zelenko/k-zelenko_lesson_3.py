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
    
file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'k-zelenko') % 23 

default_args = {
    'owner': 'k-zelenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 12)
}

CHAT_ID = 5415343789
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
    
@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False) 
def k_zelenko_lesson_3(): 
          
    @task() 
    def get_data(): 
        df_file = pd.read_csv(file)
        df = df_file[df_file.Year==year]
        return df 
      
    @task() 
    def top_game(df): 
        df_top_game = df \
            .groupby('Name') \
            .agg({'Global_Sales': 'sum'}) \
            .Global_Sales \
            .idxmax()
        return df_top_game
 
    @task() 
    def top_genre_EU(df): 
        df_top_genre_EU = df \
            .groupby('Genre') \
            .agg({'EU_Sales': 'sum'}) \
            .EU_Sales \
            .idxmax()
        return df_top_genre_EU
              
    @task() 
    def top_platform_NA(df): 
        df_top_platform_NA = df \
            .query("NA_Sales > 1") \
            .groupby('Platform') \
            .agg({'NA_Sales': 'count'}) \
            .NA_Sales \
            .idxmax()
        return df_top_platform_NA
    
    @task() 
    def top_publisher_JP(df): 
        df_top_publisher_JP = df \
            .groupby('Publisher') \
            .agg({'JP_Sales': 'mean'}) \
            .JP_Sales \
            .idxmax()
        return df_top_publisher_JP
    
    @task() 
    def count_games_better_EU(df): 
        df_count_games_better_EU = df \
            .groupby('Name') \
            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
            .query("EU_Sales > JP_Sales") \
            .shape[0]
        return df_count_games_better_EU
                                                                     
                  
    @task(on_success_callback=send_message) 
    def print_data(df_top_game, df_top_genre_EU, df_top_platform_NA, df_top_publisher_JP, df_count_games_better_EU): 
        
        context = get_current_context() 
        date = context['ds'] 
      
        print(f'In {year} the {df_top_game} was the best-selling game in the world') 
         
        print(f'{df_top_genre_EU} games were the best-selling games in Europe in {year}') 
           
        print(f'The {df_top_platform_NA} platform had the most games to sell over a million units in North America in {year}') 
      
        print(f'{df_top_publisher_JP} publisher has highest average sales in Japan in {year}') 
               
        print(f'{df_count_games_better_EU} games sold better in Europe than in Japan in {year}') 
        
    
    df = get_data()
    df_top_game = top_game(df)
    df_top_genre_EU = top_genre_EU(df)
    df_top_platform_NA = top_platform_NA(df)
    df_top_publisher_JP = top_publisher_JP(df)
    df_count_games_better_EU = count_games_better_EU(df)
    
    print_data(df_top_game, df_top_genre_EU, df_top_platform_NA, df_top_publisher_JP, df_count_games_better_EU)
             
k_zelenko_lesson_3 = k_zelenko_lesson_3() 
