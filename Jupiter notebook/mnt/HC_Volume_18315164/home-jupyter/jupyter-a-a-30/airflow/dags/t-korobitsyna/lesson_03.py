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

year = 1994 + hash(f't-korobitsyna') % 23
vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 't-korobitsyna',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 30)
}
schedule_interval = '0 12 * * *'
    
CHAT_ID = 1337145797
try:
    BOT_TOKEN = '5458430054:AAHZ_YHWIKXwSjLF6KF_Jdzg3hymBSqMHzE'
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
def tkorobitsyna_lesson_3_af():
    
    @task(retries=3)
    def get_data():
        vgsales_df = pd.read_csv(vgsales)
        return vgsales_df
    
    
    @task()
    def get_top_game(vgsales_df):
        top_game = vgsales_df.query('Year == @year') \
                                  .groupby('Name', as_index=False) \
                                  .agg({'Global_Sales':'sum'}) \
                                  .sort_values('Global_Sales', ascending=False) \
                                  .max().to_list()[0]
        return top_game
    
    
    @task()
    def get_top_genre_EU(vgsales_df):
        top_genre_EU = vgsales_df.query('Year == @year') \
                                      .groupby('Genre', as_index=False) \
                                      .agg({'EU_Sales':'sum'}) \
                                      .sort_values('EU_Sales', ascending=False) \
                                      .max().to_list()[0]
        return top_genre_EU
    
    
    @task()
    def get_top_platform_NA(vgsales_df):
        top_platform_NA = vgsales_df.query('Year == @year and NA_Sales > 1') \
                                      .groupby('Platform', as_index=False) \
                                      .agg({'Name':'count'}) \
                                      .rename(columns={'Name':'Count'}) \
                                      .sort_values('Count', ascending=False) \
                                      .max().to_list()[0]
        return top_platform_NA
    
    
    @task()
    def get_top_publisher_JP(vgsales_df):
        top_publisher_JP = vgsales_df.query('Year == @year') \
                            .groupby('Publisher', as_index=False) \
                            .agg({'JP_Sales':'mean'}) \
                            .rename(columns={'JP_Sales':'JP_Sales_mean'}) \
                            .sort_values('JP_Sales_mean', ascending=False) \
                            .max().to_list()[0] 
        return top_publisher_JP
                                      
    
    @task()
    def get_count_games_sold_better_EU_than_JP(vgsales_df):
        count_games_sold_better_EU_than_JP = vgsales_df.query('Year == @year') \
                                                       .groupby('Name', as_index=False) \
                                                       .agg({'EU_Sales':'sum', 'JP_Sales':'sum'}) \
                                                       .query('EU_Sales > JP_Sales') \
                                                       .shape[0]
        return count_games_sold_better_EU_than_JP
    
    
    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre_EU, top_platform_NA, top_publisher_JP, count_games_sold_better_EU_than_JP):

        context = get_current_context()
        date = context['ds']

        print(f'The best-selling game in {year} was {top_game} worldwide')
        print(f'The best-selling genre of game in Europe were {top_genre_EU}')
        print(f'On the platform {top_platform_NA} had the most games that sold over a million copies in North America')
        print(f'The publisher {top_publisher_JP} had the highest average sales in Japan')
        print(f'{count_games_sold_better_EU_than_JP} games have sold better in Europe than in Japan')
       
    vgsales_df = get_data()
    top_game = get_top_game(vgsales_df)
    top_genre_EU = get_top_genre_EU(vgsales_df)
    top_platform_NA = get_top_platform_NA(vgsales_df)
    top_publisher_JP = get_top_publisher_JP(vgsales_df)
    count_games_sold_better_EU_than_JP = get_count_games_sold_better_EU_than_JP(vgsales_df)
 
    print_data(top_game, top_genre_EU, top_platform_NA, top_publisher_JP, count_games_sold_better_EU_than_JP)
tkorobitsyna_lesson_3_af = tkorobitsyna_lesson_3_af()