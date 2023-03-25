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

file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'{"a.novoselov-15"}') % 23

default_args = {
    'owner': 'a.novoselov-15',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 21),
    'schedule_interval': '0 1 * * *'
}

@dag(default_args=default_args)
def a_novoselov_15_lesson3():

    @task()
    def get_data(file):
        df = pd.read_csv(file).dropna(axis='index', how='any', subset=['Year'])
        df.Year = df.Year.astype(int)
        return df

    @task()
    def get_top_sold_game(df):
        top_sold_game = df[df.Year == year]\
            .groupby('Name', as_index=False)\
            .agg({'Global_Sales':'sum'})\
            .sort_values('Global_Sales', ascending=False)
        top_sold_game_name = top_sold_game[top_sold_game.Global_Sales == top_sold_game.Global_Sales.max()].Name.to_csv(index=False, header=False)
        return top_sold_game_name

    @task()
    def get_top_sold_EU_genre(df):
        top_sold_genre = df[df.Year == 2014]\
            .groupby('Genre', as_index=False)\
            .agg({'EU_Sales':'sum'})\
            .sort_values('EU_Sales', ascending=False)
        top_sold_genre_name = top_sold_genre[top_sold_genre.EU_Sales == top_sold_genre.EU_Sales.max()].Genre.to_csv(index=False, header=False)
        return top_sold_genre_name

    @task()
    def get_top_platform(df):
        top_platform = df[df['NA_Sales'] > 1]\
            .groupby("Platform",as_index=False)\
            .agg({'NA_Sales':'count'})\
            .sort_values('NA_Sales',ascending=False)
        platform = top_platform[top_platform.NA_Sales == top_platform.NA_Sales.max()].Platform.to_csv(index=False, header=False)
        return platform

    @task()
    def get_top_publisher(df):
        top_publisher = df\
            .groupby("Publisher",as_index=False)\
            .agg({'JP_Sales':'mean'})\
            .sort_values('JP_Sales',ascending=False)
        publisher = top_publisher[top_publisher['JP_Sales']==top_publisher.JP_Sales.max()].Publisher.to_csv(index=False, header=False)
        return publisher

    @task()
    def get_best_EU_games(df):
        top_EU_games = df\
            .groupby('Name',as_index=False)\
            .agg({'EU_Sales':sum, 'JP_Sales':sum})
        number_of_games = top_EU_games[top_EU_games['EU_Sales'] > top_EU_games['JP_Sales']].shape[0]
        return number_of_games

    @task()
    def print_data(top_sold_game_name, top_sold_genre_name, platform, publisher, number_of_games):
        print(f'The best-selling game in {year} in the world: {top_sold_game_name}')
        print(f'The best-selling genre in {year} in Europe: {top_sold_genre_name}')
        print(f'Top platform games with a million copie in {year} in NA: {platform}')
        print(f'Publishers with the highest average sales in {year} in Japan: {publisher}')
        print(f'The number of games sold is better in Europe than in Japan in {year}: {number_of_games}')
        
    data = get_data(file)
    top_sold_game = get_top_sold_game(data)
    top_sold_EU_genre = get_top_sold_EU_genre(data)
    top_platform = get_top_platform(data)
    top_publisher = get_top_publisher(data)
    best_EU_games = get_best_EU_games(data)   
    print_data(top_sold_game, top_sold_EU_genre, top_platform, top_publisher, best_EU_games)

a_novoselov_15_lesson3 = a_novoselov_15_lesson3()
