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
    'owner': 'l-sharipkov-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 16),
    'schedule_interval': '0 16 * * *'
}



@dag(default_args=default_args, catchup=False)
def games_2009_airflow():
    @task()
    def get_data():
        data = pd.read_csv('/mnt/HC_Volume_18315164/home-jupyter/jupyter-l-sharipkov-25/g/Airflow/vgsales.csv')
        year = 1994 + hash(f'l-sharipkov-25') % 23
        my_data = df.query('Year == @year')
        
        return my_data

    @task()
    def get_best_sales_game(my_data):
        best_sales_game = my_data.groupby('Name', as_index=False).agg({'Global_Sales': 'sum'}) \
                  .sort_values('Global_Sales', ascending=False).iloc[0,0]
        return best_sales_game
    
    @task()
    def get_best_EU_genre(my_data):
        best_EU_genre = my_data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}) \
                               .sort_values('EU_Sales', ascending=False).iloc[0,0]
        return best_EU_genre
    
    @task()
    def get_best_platform(my_data):
        best_platform = my_data.query('NA_Sales >= 1').groupby('Platform', as_index=False).agg({'NA_Sales': 'count'}) \
                               .sort_values('NA_Sales', ascending=False).iloc[0,0]
        return best_platform
    
    @task()
    def get_best_JP_publisher(my_data):
        best_JP_publisher = my_data.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}) \
                                   .sort_values('JP_Sales', ascending=False).iloc[0,0]
        return best_JP_publisher
    
    @task()
    def get_how_many_games_sales_betterEU(my_data):
        how_many_games_sales_betterEU = my_data.query('EU_Sales > JP_Sales').shape[0]
        
        return how_many_games_sales_betterEU

    @task()
    def print_data(best_sales_game_stat, best_EU_genre_stat,
                   best_platform_stat, best_JP_publisher_stat,
                   how_many_games_sales_betterEU_stat):

        context = get_current_context()
        date = context['ds']

        ru_avg, ru_median = ru_stat['ru_avg'], ru_stat['ru_median']
        com_avg, com_median = com_stat['com_avg'], com_stat['com_median']
        
        p_best_sales_game = best_sales_game_stat
        p_best_EU_genre = best_EU_genre_stat
        p_best_platform = best_platform_stat
        p_best_JP_publisher = best_JP_publisher_stat
        p_how_many_games_sales_betterEU = how_many_games_sales_betterEU_stat
        
        

        print(f'''Data for {date}
                  The best-selling game of the year worldwide: {p_best_sales_game}
                  The most popular game genre in Europe: {p_best_EU_genre}
                  The platform with the most games sold in millions of copies in NA: {p_best_platform}
                  The publisher with the highest average sales in Japan: {p_best_JP_publisher}
                  This number of games sold better in Europe than in Japan: {p_how_many_games_sales_betterEU}''')



    top_data = get_data()
    top_data_ru = get_table_ru(top_data)
    ru_data = get_stat_ru(top_data_ru)

    top_data_com = get_table_com(top_data)
    com_data = get_stat_com(top_data_com)

    print_data(ru_data, com_data)
    
    games = get_data()
    best_sales_game1 = get_best_sales_game(games)
    best_EU1 = get_best_EU_genre(games)
    best_platform1 = get_best_platform(games)
    best_JP_publisher1 = get_best_JP_publisher(games)
    how_many_games_sales_betterEU1 = get_how_many_games_sales_betterEU(games)
    
    print_data(best_sales_game1, best_EU1,
                   best_platform1, best_JP_publisher1,
                   how_many_games_sales_betterEU1)
    
    

games_2009_airflow = games_2009_airflow()