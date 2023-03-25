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

default_args = {
    'owner': 'i-matvijchuk-28',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 2)
}    
schedule_interval = '0 10 * * *'

vg_sales_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
analysis_year = 1994 + hash('i-matvijchuk-28') % 23


@dag(default_args=default_args, catchup=False)
def i_matvijchuk_28_lesson_3():

    @task(retries=4, retry_delay=timedelta(10))    
    def get_data():
        file = vg_sales_file
        vg_sales_df = pd.read_csv(file).query('Year == @analysis_year')
        return vg_sales_df

    @task()
    def get_top_sales_game(vg_sales_df):
        top_sales_game = vg_sales_df \
            .groupby('Name', as_index=False) \
            .agg({'Global_Sales':'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .head(1)        
        top_sales_game = top_sales_game.Name.values[0]
        return top_sales_game

    @task()
    def get_top_genre_europe(vg_sales_df):
        top_sales_genre_europe = vg_sales_df \
            .groupby('Genre', as_index=False) \
            .agg({'EU_Sales':'sum'}) \
            .sort_values('EU_Sales', ascending=False) \
            .head(5)
        top_sales_genre_europe = top_sales_genre_europe.Genre.to_list()
        return top_sales_genre_europe

    @task()
    def get_top_platform_na(vg_sales_df):
        top_sales_platform_na = vg_sales_df \
            .groupby(['Platform', 'Name'], as_index=False) \
            .agg({'NA_Sales':'sum'}) \
            .query("NA_Sales >= 1") \
            .groupby('Platform', as_index=False) \
            .agg({'Name':'count'}) \
            .sort_values('Name', ascending=False) \
            .rename(columns={'Name': 'Games_with_1M_sales'}) \
            .head(5)
        top_sales_platform_na = top_sales_platform_na.Platform.to_list()
        return top_sales_platform_na      

    @task()
    def get_top_publisher_jp(vg_sales_df):
        top_sales_publisher_jp = vg_sales_df \
            .groupby('Publisher', as_index=False) \
            .agg({'JP_Sales':'mean'}) \
            .sort_values('JP_Sales', ascending=False) \
            .head(5)
        top_sales_publisher_jp = top_sales_publisher_jp.Publisher.to_list()
        return top_sales_publisher_jp       

    @task()
    def get_top_game_eu_jp(vg_sales_df):
        top_sales_game_eu_jp = vg_sales_df \
            .groupby('Name', as_index=False) \
            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'}) \
            .query("EU_Sales > JP_Sales") \
            .sort_values('EU_Sales', ascending=False) \
            .shape[0]
        return top_sales_game_eu_jp

    @task()
    def print_data(top_sales_game,top_sales_genre_europe,top_sales_platform_na,top_sales_publisher_jp,top_sales_game_eu_jp):      
        print(f'Cамая продаваемая игра в {analysis_year} году во всем мире:', top_sales_game)
        print(f'Игры этого жанра были самыми продаваемыми в Европе в {analysis_year} году:', top_sales_genre_europe)
        print(f'На этой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {analysis_year} году:', top_sales_platform_na)
        print(f'У этого издателя самые высокие средние продажи в Японии в {analysis_year} году:', top_sales_publisher_jp)
        print(f'Столько игр продались лучше в Европе, чем в Японии в {analysis_year} году:', top_sales_game_eu_jp)    

    vg_data = get_data()
    sales_game = get_top_sales_game(vg_data)
    genre_europe = get_top_genre_europe(vg_data)
    platform_na = get_top_platform_na(vg_data)
    publisher_jp = get_top_publisher_jp(vg_data)
    game_eu_jp = get_top_game_eu_jp(vg_data)   

    print_data(sales_game, genre_europe, platform_na, publisher_jp, game_eu_jp)
    
i_matvijchuk_28_lesson_3 = i_matvijchuk_28_lesson_3()