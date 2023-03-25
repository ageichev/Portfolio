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

link = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'm.gareeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 22),
    'schedule_interval': '0 10 * * *'
}

@dag(default_args=default_args, catchup=False)
def lesson3_airflow():

    @task()
    def get_year(): # определение года
        df = pd.read_csv(link)
        year = 1994 + hash(f'm-gareeva') % 23  # определим год, за который будем смотреть данные
        return year
    
    @task()
    def get_data(year): # считывание данных
        df = pd.read_csv(link)
        df_year = df.loc[df.Year == year]
        return df_year

    @task() # Какая игра была самой продаваемой в этом году во всем мире?
    def get_most_popular_game(df_year):
        most_popular_game = df_year.groupby('Name', as_index=False).agg({'Global_Sales': 'sum'}).Global_Sales.max()
        return most_popular_game

    @task() #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_best_selling_game(df_year):
        best_selling_game = df_year.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).nlargest(1, 'EU_Sales', keep='all').Genre.to_list()
        return best_selling_game

    @task() #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_platform(df_year):
        platform = df_year.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'Name': 'nunique'}).nlargest(1, 'Name', keep='all').Platform.to_list()
        return platform

    @task() # У какого издателя самые высокие средние продажи в Японии?
    def get_publisher(df_year):
        publisher = df_year.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'}).nlargest(1, 'JP_Sales', keep='all').Publisher.to_list()
        return publisher
    
    @task() # Сколько игр продались лучше в Европе, чем в Японии?
    def get_count_game(df_year):
        count_game = df_year.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).query('EU_Sales > JP_Sales').Name.count()
        return count_game

    @task()
    def print_data(df_year, year, most_popular_game, best_selling_game, platform, publisher, count_game):
        print(f'Data for the year: {year}')
        print(f'The most popular game: {most_popular_game}')
        print(f'Genre of the best selling game in EU: {best_selling_game}')
        print(f'Platform with the highest 1 mln selling game in NA: {platform}')
        print(f'The publisher with the highest average sales in JP: {publisher}')
        print(f'Count games sold better in EU than in JP: {count_game}')        
       
    year = get_year()
    df_year = get_data(year)
    most_popular_game = get_most_popular_game(df_year)
    best_selling_game = get_best_selling_game(df_year)
    platform = get_platform(df_year)
    publisher = get_publisher(df_year)
    count_game = get_count_game(df_year)
    print_data(df_year, year, most_popular_game, best_selling_game, platform, publisher, count_game)
    
mgareeva_lesson_3 = lesson3_airflow()
