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

# initialization
VGSALES_DATA = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'j-kutakova'
year = 1994 + hash(login) % 23

# create dag
default_args = {
    'owner': 'j-kutakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 9, 1),
    'schedule_interval': '0 10 * * *'
}


# dag
@dag(default_args=default_args, catchup=False)
def j_kutakova_21_airflow_3():
    # get data from vgsales
    @task(retries=2)
    def get_data(data, year):
        df = pd.read_csv(data).query('Year==@year')
        return df

    # 1.The best selling game in the world
    @task(retries=1)
    def get_top_global_game(df):
        top_global_game = (
            df.groupby('Name')
                .agg({'Global_Sales': 'sum'})['Global_Sales'].idxmax()
            )
        return {'top_global_game': top_global_game}

    # 2.The best selling genre in Europe
    @task(retries=1)
    def get_top_europe_genre(df):
        top_europe_genre = (
            df.groupby('Genre', as_index=False)
                .agg({'EU_Sales': 'sum'})
        )
        top_europe_genre = top_europe_genre[top_europe_genre['EU_Sales'] == top_europe_genre['EU_Sales'].max()]['Genre'].to_list()
        return {'top_europe_genre': top_europe_genre}

    # 3. Platform with the biggest amount of games that were sold over a million copies in North America
    @task(retries=1)
    def get_top_platform_NA(df):
        top_platform_NA = (
            df.query('NA_Sales > 1').groupby('Platform', as_index=False)
                .agg({'Name': 'nunique'})
                .rename(columns={'Name': 'cnt_unique'})
        )
        top_platform_NA = top_platform_NA[top_platform_NA['cnt_unique'] == top_platform_NA['cnt_unique'].max()]['Platform'].to_list()
        return {'top_platform_NA': top_platform_NA}

    # 4. Publisher with the highest average sales in Japan
    @task(retries=1)
    def get_top_publisher_JP(df):
        top_publisher_JP = (
            df.groupby('Publisher', as_index=False)
                .agg({'JP_Sales': 'mean'})
        )
        top_publisher_JP = top_publisher_JP[top_publisher_JP['JP_Sales'] == top_publisher_JP['JP_Sales'].max()]['Publisher'].to_list()
        return {'top_publisher_JP': top_publisher_JP}

    # 5. The number of games thhat sold over better in Europe than in Japan
    @task(retries=1)
    def get_top_europe_games(df):
        top_europe_games = (
            df.groupby('Name', as_index=False)
                .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
                .query('EU_Sales > JP_Sales').count().iloc[0]
        )
        return {'top_europe_games': top_europe_games}

    @task(retries=1)
    def print_data(top_global_game, top_europe_genre, top_platform_NA, top_publisher_JP, top_europe_games):
        context = get_current_context()
        date = context['ds']

        top_global_game = top_global_game['top_global_game']
        top_europe_genre = ', '.join(top_europe_genre['top_europe_genre'])
        top_platform_NA = ', '.join(top_platform_NA['top_platform_NA'])
        top_publisher_JP = ', '.join(top_publisher_JP['top_publisher_JP'])   
        top_europe_games = top_europe_games['top_europe_games']    

        print(f"The best selling game in the world for {year} was {top_global_game}")
        print(f"The best selling genre in Europe for {year} was {top_europe_genre}")
        print(f"Platform with the biggest amount of games that were sold over a million copies in North America for {year} was {top_platform_NA}")
        print(f"Publisher with the highest average sales in Japan for {year} was {top_publisher_JP}")
        print(f"The number of games thhat sold over better in Europe than in Japan for {year} was {top_europe_games}")

    df = get_data(VGSALES_DATA, year)
    top_global_game_data = get_top_global_game(df)
    top_europe_genre_data = get_top_europe_genre(df)
    top_platform_NA_data = get_top_platform_NA(df)
    top_publisher_JP_data = get_top_publisher_JP(df)
    top_europe_games_data = get_top_europe_games(df)
    
    print_data(top_global_game_data, top_europe_genre_data, top_platform_NA_data, top_publisher_JP_data, top_europe_games_data)


j_kutakova_21_airflow_3 = j_kutakova_21_airflow_3()