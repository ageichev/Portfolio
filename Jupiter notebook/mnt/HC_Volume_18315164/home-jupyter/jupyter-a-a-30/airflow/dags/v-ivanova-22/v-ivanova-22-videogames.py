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

year = 1994 + hash(f'{"v-ivanova-22"}') % 23
path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'v-ivanova-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 2),
    'schedule_interval': '0 8 * * *'
}


@dag(default_args=default_args, catchup=False)
def v_ivanova_22_videogames():
    @task()
    def get_data():
        df = pd.read_csv(path)
        df = df.query('Year==@year')
        return df

# 1.Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_game(df):
        best_game = df.groupby('Name', as_index=False).agg({'Global_Sales':'sum'})\
                    .sort_values('Global_Sales', ascending=False)\
                    .reset_index().loc[0]['Name']
        return best_game

# 2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько    
    @task()
    def get_EU_best_genre(df):
        EU_most_popular_genres = df.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'})\
                                    .sort_values('EU_Sales', ascending=False)\
                                    .reset_index().loc[0]['Genre']
        return EU_most_popular_genres

# 3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?     
    @task()
    def get_best_NA_platform(df):
        NA_best_platform = df.query('NA_Sales>1').groupby('Platform', as_index=False).agg({'Name':'count'})\
                            .sort_values('Name', ascending=False)\
                            .reset_index()\
                            .query('Name == Name.max()')['Platform'].values
        return NA_best_platform

# 4. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_JP_best_publisher(df):
        JP_best_publisher = df.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'})\
                            .sort_values('JP_Sales', ascending=False)\
                            .reset_index()\
                            .query('JP_Sales == JP_Sales.max()')['Publisher'].values
        return JP_best_publisher
    
# 5. Сколько игр продались лучше в Европе, чем в Японии?    
    @task()
    def get_EU_beats_JP(df):
        EU_beats_JP = df.groupby('Name', as_index=False).sum().reset_index()\
                        .query('EU_Sales>JP_Sales').Name.nunique()
        return EU_beats_JP

    @task()
    def print_data(get_best_game, get_EU_best_genre, get_best_NA_platform, get_JP_best_publisher, get_EU_beats_JP):

        context = get_current_context()
        date = context['ds']

        #1
        print(f'Game with best sales in {year} was:')
        print(best_game)

        #2
        print(f'Most popular genre(s) in EU in {year} was (were):')
        print(EU_most_popular_genres)
        
        #3
        print(f'Most popular platform(s) in NA in {year} was (were):')
        print(NA_best_platform)
        
        #4
        print(f'Publisher(s) with higest mean sales in JP in {year} was (were):')
        print(JP_best_publisher)
        
        #5
        print(f'Quantity of games where sales in EU are higher then in JP in {year} is:')
        print(EU_beats_JP)


    df = get_data()
    best_game = get_best_game(df)
    EU_most_popular_genres = get_EU_best_genre(df)
    NA_best_platform = get_best_NA_platform(df)
    JP_best_publisher = get_JP_best_publisher(df)
    EU_beats_JP = get_EU_beats_JP(df)

    print_data(get_best_game, get_EU_best_genre, get_best_NA_platform, get_JP_best_publisher, get_EU_beats_JP)

v_ivanova_22_videogames = v_ivanova_22_videogames()