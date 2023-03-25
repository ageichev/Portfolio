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

GAME_SALES_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
my_login = 'a-gorbanev-26'
year = 1994 + hash(my_login) % 23

default_args = {
    'owner': 'a.gorbanev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 23),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def a_gorbanev_lesson03():
    @task(retries=3)
    def get_data():
        game_data = pd.read_csv(GAME_SALES_FILE)        
        game_data.columns = game_data.columns.str.lower()
        game_data = game_data[game_data['year'] == year].copy()
        return game_data

    @task()
    def get_top_game(game_data):
        top_game = game_data[game_data['global_sales'] == game_data['global_sales'].max()]['name']
        return top_game.to_csv(index=False)

    @task()
    def get_top_genre_eu(game_data):
        top_genre_eu = game_data[game_data.eu_sales == game_data.eu_sales.max()].genre
        return top_genre_eu.to_csv(index=False)

    @task()
    def get_top_platform_na(game_data):
        top_platform_na = game_data[game_data['na_sales'] > 1.0]\
                            .groupby('platform', as_index=False)\
                            .agg({'name': 'count'})\
                            .rename(columns={'name': 'quantity'})
        top_platform_na = top_platform_na[top_platform_na['quantity'] == top_platform_na['quantity'].max()]
        return top_platform_na.to_csv(index=False)

    @task()
    def get_japane_mean(game_data):
        japane_mean = game_data.groupby('publisher', as_index=False).agg({'jp_sales': 'mean'})
        japane_mean = japane_mean[japane_mean['jp_sales'] == japane_mean['jp_sales'].max()]
        return japane_mean.to_csv(index=False)

    @task()
    def get_eu_vs_japane(game_data):
        eu_vs_japane = game_data[game_data['eu_sales'] > game_data['jp_sales']].shape[0]
        return eu_vs_japane
    
    @task()
    def print_data(top_game, top_genre_eu, top_platform_na, japane_mean, eu_vs_japane):

        context = get_current_context()
        date = context['ds']
        
        print(f'Current date {date}')
        
        print(f'''The world's best-selling game in {year}
                  {top_game}
                  ''')

        print(f'''The best-selling game genre in Europe in {year}
                          {top_genre_eu}''')
        
        print(f'''Top game platform in North America in {year}
                          {top_platform_na}''')
        
        print(f'''Publisher with the highest average sales in Japan in {year}
                          {japane_mean}''')
        
        print(f'''Number of games that sold better in Europe than in Japan in {year}
                          {eu_vs_japane}''')


    game_data = get_data()
    top_game = get_top_game(game_data)
    top_genre_eu = get_top_genre_eu(game_data)
    top_platform_na = get_top_platform_na(game_data)
    japane_mean = get_japane_mean(game_data)
    eu_vs_japane = get_eu_vs_japane(game_data)

    print_data(top_game, top_genre_eu, top_platform_na, japane_mean, eu_vs_japane)

a_gorbanev_lesson03 = a_gorbanev_lesson03()