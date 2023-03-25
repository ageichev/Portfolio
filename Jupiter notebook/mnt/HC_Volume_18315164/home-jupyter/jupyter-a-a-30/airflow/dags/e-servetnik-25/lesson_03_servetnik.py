import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


VGSALES_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'e-servetnik-25'
year = 1994 + hash(f'{login}') % 23
print(year)

default_args = {
    'owner': 'e.servetnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 20),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def game_data():
                   
    @task(retries=3)
    def get_data():
        top_data = pd.read_csv(VGSALES_FILE)
        top_data = top_data[top_data['Year'] == year]
        return top_data

    @task()
    def get_top_game(top_data):
        df_top_game = df.groupby('Name').agg({'Global_Sales': 'sum'}).idxmax()
        return df_top_game.to_csv(index=False)
    
    @task()
    def get_top_genre_EU(top_data):
        df_top_genre_EU = df.groupby('Genre').agg({'EU_Sales': 'sum'}).idxmax()
        return df_top_genre_EU.to_csv(index=False)

    @task()
    def get_top_Publisher_JP(top_data):
        df_Publisher_JP = df.groupby('Publisher').agg({'JP_Sales': 'mean'}).idxmax()
        return df_Publisher_JP.to_csv(index=False)
  
    @task()
    def get_top_Platform(top_data):
        df_Platform = df.groupby(['Platform']).agg({'NA_Sales': 'sum'}).idxmax()
        return df_Platform.to_csv(index=False)          
   
    @task()
    def get_games_EU_vs_JP(top_data):
        df_Games_EU = df.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        df_Games_EU['compare'] = df_Games_EU['EU_Sales'] > df_Games_EU['JP_Sales']
        df_Games = df_Games_EU[df_Games_EU['compare'] == True]
        df_Games = df_Games.Name.nunique()
        return df_Games.to_csv(index=False)     

    @task
    def print_data(df_top_game, df_top_genre_EU, df_Publisher_JP, df_Platform, df_Games):
        context = get_current_context()
        date = context['ds']
            
        print(f'''The top game in the world for {year} is {df_top_game}''')        
        print(f'''The top genre in EU for {year} is {df_top_genre_EU}''')                   
        print(f'''The top publisher in JP for {year} is {df_Publisher_JP}''')
        print(f'''The top platform in NA for {year} is {df_Platform}''')
        print(f'''The quantity of games with EU sales more then JP sales for {year} is {df_Games}''')
                             
                   
    top_data = get_data()
    df_top_game = get_top_game(top_data)
    df_top_genre_EU = get_top_genre_EU(top_data)
    df_Publisher_JP = get_top_Publisher_JP(top_data)
    df_Platform = get_top_Platform(top_data)
    df_Games = get_games_EU_vs_JP(top_data)
    print_data(df_top_game, df_top_genre_EU, df_Publisher_JP, df_Platform, df_Games)

game_data = game_data()