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

games = 'vgsales.csv'
year = 1994 + hash(f'n-lopatkov-27') % 23

default_args = {
    'owner': 'n.lopatkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 12, 24)
}

schedule_interval = '05 14 * * *'


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def top_games_nlopatkov(): 
    @task(retries=3)
    def get_games_data():
        games_data = pd.read_csv(games).query("Year == @year")
        return games_data
    
    @task(retries=3)
    def get_best_game_of_the_year(games_data):
        best_game_of_the_year = games_data \
                    .sort_values('Global_Sales', ascending=False) \
                    .head(1).Name.to_string()
        return best_game_of_the_year

    @task(retries=4)
    def get_best_genres_eu(games_data):
        best_genres_eu = list(games_data.nlargest(1, 'EU_Sales', keep='all') \
                                            .Genre.unique())
        return best_genres_eu

    @task()
    def get_top_platforms(games_data):
        top_platforms = games_data.query("NA_Sales > 1") \
                                        .Platform.value_counts() \
                                        .nlargest(1, keep='all') \
                                        .index.to_list()
        return top_platforms

    @task()
    def get_top_sales_jp(games_data):
        top_sales_jp = games_data.groupby('Publisher', as_index=False) \
                                        .agg({'JP_Sales': 'mean'}) \
                                        .nlargest(1, 'JP_Sales', keep='all') \
                                        .Publisher.to_list()
        return top_sales_jp

    @task()
    def get_eu_jp(games_data):
        eu_jp = games_data.query("EU_Sales > JP_Sales").shape[0]
        return eu_jp

    @task()
    def print_data(best_game_of_the_year, 
                   best_genres_eu, 
                   top_platforms,
                   top_sales_jp,
                   eu_jp):


        print(f'Год {year}:')
              
        print(f'Самая продаваемая игра во всем мире: {best_game_of_the_year}')

        print(f"Самый продаваемый жанр(-ы) Европе: {', '.join(best_genres_eu)}")
        
        print(f"Игры в Северной Америке > 1 миллиона на платформе(-ах) {', '.join(top_platforms)}")
              
        print(f"Самые высокие средние продажи в Японии у издателя {', '.join(top_sales_jp)}")
              
        print(f'Количество игр, которые проданы в Европе лучше, чем в Японии: {eu_jp}')

    games_data = get_games_data()
    
    best_game_of_the_year = get_best_game_of_the_year(games_data)
    best_genres_eu = get_best_genres_eu(games_data)
    top_platforms = get_top_platforms(games_data)
    top_sales_jp = get_top_sales_jp(games_data)
    eu_jp = get_eu_jp(games_data)

    print_data(best_game_of_the_year, best_genres_eu, top_platforms, top_sales_jp, eu_jp)

top_games_nlopatkov = top_games_nlopatkov()

