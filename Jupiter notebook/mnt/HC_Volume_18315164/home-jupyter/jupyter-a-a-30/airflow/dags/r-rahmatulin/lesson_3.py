
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


#____________________

path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'r-rakhmatulin'
my_year = 1994 + hash(f"{login}") % 23

default_args = {
    'owner': 'r-rakhmatulin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 24)
}


#____________________


@dag(default_args=default_args, schedule_interval='0 9 * * *', catchup=False)

def rrahmatulin_task():
    
    @task()
    def get_data():
        df = pd.read_csv(path_to_file)
        df_year = df[df.Year == my_year]
        return df_year
    
#____________________
    
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_game(df_year):
        top_game = df_year.groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False).head(1).Name.tolist()[0]
        return top_game
    
#____________________  


    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_top_genre(df_year):
        top_genre = df_year.groupby('Genre', as_index=False) \
            .agg({'EU_Sales':'sum'}) \
            .sort_values('EU_Sales', ascending=False) \
            .head(1).Genre.tolist()[0]
        return top_genre

#____________________ 


    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    # Перечислить все, если их несколько
    @task()
    def get_top_platform(df_year):
        top_platform = df_year.groupby('Platform', as_index=False) \
            .agg({'NA_Sales':'sum'}) \
            .query('NA_Sales > 1') \
            .sort_values('NA_Sales', ascending=False).Platform.tolist()
        return top_platform
    
#____________________


    # У какого издателя самые высокие средние продажи в Японии? 
    # Перечислить все, если их несколько
    @task()
    def get_top_publisher(df_year):
        top_publisher = df_year.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales':'mean'}) \
            .sort_values('JP_Sales', ascending=False).head(1).Publisher.tolist()[0]
        return top_publisher
    
#____________________


    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_europe_japan_game(df_year):
        europe_japan_game = df_year.groupby('Name', as_index=False) \
            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'}) \
            .query('EU_Sales > JP_Sales').shape[0]
        return europe_japan_game
    
#____________________
    
    
    @task()
    def print_data(top_game, top_genre, top_platform, top_publisher, europe_japan_game):
        print(f'''
            Top game in {my_year}: {top_game}
            Top genre in Europe in {my_year}: {top_genre}
            Top platform in North America in {my_year}: {top_platform}
            Top publisher in Japan in {my_year}: {top_publisher}
            in {my_year}, europe sold {europe_japan_game} more games than japan''')
            
#____________________
             
              
    df_year = get_data()
    top_game = get_top_game(df_year)
    top_genre = get_top_genre(df_year)
    top_platform = get_top_platform(df_year)
    top_publisher = get_top_publisher(df_year)
    europe_japan_game = get_europe_japan_game(df_year)

    print_data(top_game, top_genre, top_platform, top_publisher, europe_japan_game)

        
rrahmatulin_task = rrahmatulin_task()

