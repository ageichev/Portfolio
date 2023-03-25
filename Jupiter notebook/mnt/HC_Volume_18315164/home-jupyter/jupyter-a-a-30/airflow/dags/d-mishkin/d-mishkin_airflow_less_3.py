import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO # StringIO    
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable #     

GAMES_SALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'd-mishkin') % 23


default_args = {
    'owner': 'd.mishkin', 
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 28),
    'schedule_interval': '0 24 * * *' 
}

CHAT_ID = 324029589 
try:
    BOT_TOKEN = '5747740219:AAFjx2j-Fm2fseQKyYbkfmfevpmyH7HLdFU'
except: #   
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
def d_mishkin_games_sales():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(GAMES_SALES)
        games_df = df.query('Year==@year')
        return games_df

    @task()
    def get_world_best_selling_game(games_df):   
        world_best_selling_game = games_df.groupby('Name').agg({'Global_Sales': 'sum'})\
                                            .sort_values('Global_Sales', ascending=False).head(1).index.array[0]
        return world_best_selling_game
    

    @task(retries=3)
    def get_top_genre_EU(games_df):
        max_EU_sales = games_df.groupby('Genre').agg({'EU_Sales': 'sum'}).max().values[0]
        top_genre_EU = games_df.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})\
                                .query('EU_Sales==@max_EU_sales').Genre.iloc[0]
        return top_genre_EU


    @task()
    def get_top_platform_NA(games_df):
        max_game_count_NA = games_df.query('NA_Sales>1').groupby('Platform').agg({'Name': 'count'}).max().values[0]
        top_platform_NA = games_df.query('NA_Sales>1').groupby('Platform', as_index=False).agg({'Name': 'count'})\
                                    .query('Name==@max_game_count_NA').Platform.iloc[0]
        return top_platform_NA

    
    @task()
    def get_sales_JP(games_df):
        sales_JP = games_df.groupby('Publisher').agg({'JP_Sales': 'mean'})\
                            .sort_values('JP_Sales', ascending=False).head(1).index.array[0]
        return sales_JP


    @task()
    def get_count_games(games_df):
        count_games = games_df.groupby('Name').agg({'JP_Sales': 'sum', 'EU_Sales': 'sum'}).query('EU_Sales>JP_Sales').shape[0]
        return count_games

   
  
    @task()
    def print_data(world_best_selling_game, top_genre_EU, top_platform_NA, sales_JP, count_games):
        context = get_current_context()
        date = context['ds']

        print(f'''World best selling game in {year}: {world_best_selling_game}
                  Top genre EU in {year}: {top_genre_EU}
                  Top platform NA in {year}: {top_platform_NA}
                  Publisher with the highest average sales in Japan in {year}: {sales_JP}
                  Number of games that have sold better in Europe than in Japan in {year}: {count_games}''')

       
    games_df = get_data()
    world_best_selling_game = get_world_best_selling_game(games_df)
    top_genre_EU = get_top_genre_EU(games_df)
    top_platform_NA = get_top_platform_NA(games_df)
    sales_JP = get_sales_JP(games_df)
    count_games = get_count_games(games_df)

    print_data(world_best_selling_game, top_genre_EU, top_platform_NA, sales_JP, count_games)

d_mishkin_games_sales = d_mishkin_games_sales()
