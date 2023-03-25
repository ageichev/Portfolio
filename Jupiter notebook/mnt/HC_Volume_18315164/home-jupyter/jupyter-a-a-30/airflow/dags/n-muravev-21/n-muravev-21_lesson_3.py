import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from urllib.parse import urlencode

from airflow.decorators import dag, task

game_data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'n-muravev-21'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'n-muravev-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 30),
    'schedule_interval' : '0 19 * * *'
}

@dag(default_args=default_args, catchup=False)
def n_muravev_21_lesson_3_exercise():

    @task()
    def get_data():
        data = pd.read_csv(game_data)
        data = data[data.Year == year]
        return data

    @task()
    def best_global_seller_game(data):
        task_1 = data[data.Global_Sales == data.Global_Sales.max()].Name
        return task_1

    @task()
    def best_europe_seller_genre(data):
        task_2 = data.groupby('Genre', as_index=0).agg({'EU_Sales': 'mean'}).sort_values('EU_Sales', ascending=False).head(1).Genre
        return task_2

    @task()
    def best_north_america_platform(data):
        task_3 = data[data.NA_Sales > 1].groupby('Platform', as_index=0).agg({'Name': 'count'}).sort_values('Name', ascending=False).head(1).Platform
        return task_3

    @task()
    def pubilisher_with_best_sales_in_japan(data):
        task_4 = data.groupby('Publisher', as_index=0).agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).head(1).Publisher
        return task_4

    @task()
    def number_of_games_with_more_sales_in_europe_than_in_japan(data):
        task_5 = data[data.EU_Sales > data.JP_Sales].shape[0]
        return task_5

    @task()
    def print_data(task_1, task_2, task_3, task_4, task_5):
        
        print(f'''Data for {year} year
                        The best global seller: {task_1}
                        The best seller genre in Europe: {task_2}
                        The platform with the largest number of games sold over one million in North America: {task_3}
                        The publisher with the largest mean sales in Japan: {task_4}
                        The number of games sold in Europe better than in Japan: {task_5}''')
        
    data = get_data()

    game = best_global_seller_game(data)
    genre = best_europe_seller_genre(data)
    platform = best_north_america_platform(data)
    publisher = pubilisher_with_best_sales_in_japan(data)
    amount = number_of_games_with_more_sales_in_europe_than_in_japan(data)
        
    print_data(game, genre, platform, publisher, amount)
    
n_muravev_21_lesson_3_exercise = n_muravev_21_lesson_3_exercise()