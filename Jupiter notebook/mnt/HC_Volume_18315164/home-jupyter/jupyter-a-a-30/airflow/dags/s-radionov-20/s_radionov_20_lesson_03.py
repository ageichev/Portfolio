import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram
import requests
import json
from urllib.parse import urlencode

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 's-radionov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 8),
    'schedule_interval': '0 08 * * *'
}

 
year = (1994 + hash(f's-radionov-20') % 23)
    
@dag(default_args=default_args, catchup=False)
def semradionov2():
    @task(retries=2)
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df_year = df.query('Year == @year')
        return df_year

    
    @task()
    def most_sales_game(df_year):
        most_sales_game = df_year \
            .groupby('Name', as_index = False) \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values(by=['Global_Sales'], ascending = False) \
            .head(1) \
            .reset_index() \
            .Name \
            .values[0]
        return most_sales_game


    @task()
    def most_sales_genre_eu(df_year):  
        sales_eu = df_year \
            .groupby('Genre', as_index = False) \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values(by=['EU_Sales'], ascending = False)
        most_sales_eu = sales_eu.EU_Sales.max()
        most_sales_genre_eu = sales_eu.query('EU_Sales == @most_sales_eu').reset_index().Genre.values[0]
        return most_sales_genre_eu

    
    @task()
    def most_count_games_platform_na(df_year):      
        sales_na = df_year \
            .query('NA_Sales > 1') \
            .groupby('Platform', as_index = False) \
            .agg({'Name': 'count'}) \
            .rename(columns={'Name':'Games_count'}) \
            .sort_values(by=['Games_count'], ascending = False)
        most_count_games_na = sales_na.Games_count.max()
        most_count_games_platform_na = sales_na.query('Games_count == @most_count_games_na').reset_index().Platform.values[0]
        return most_count_games_platform_na


    @task()
    def most_avg_sales_publisher_jp(df_year):     
        sales_jp = df_year \
            .groupby('Publisher', as_index = False) \
            .agg({'JP_Sales':'mean'}) \
            .rename(columns={'JP_Sales':'Avg_JP_sales'}) \
            .sort_values(by=['Avg_JP_sales'], ascending = False)
        most_avg_sales_jp = sales_jp.Avg_JP_sales.max()
        most_avg_sales_publisher_jp = sales_jp.query('Avg_JP_sales == @most_avg_sales_jp').reset_index().Publisher.values[0]
        return most_avg_sales_publisher_jp

    
    @task()
    def game_count_more_popular_in_eu(df_year): 
        game_count_more_popular_in_eu = df_year.query('EU_Sales > JP_Sales').Name.nunique()
        return game_count_more_popular_in_eu


    @task()
    def print_data(top_sales, top_genre_eu, top_platform_na, top_avg_sales_publisher_jp, popular_in_eu):

        context = get_current_context()
        date = context['ds']

        print(f'Cамой продаваемой во всем мире игрой в {year} была {top_sales}')
        print(f'Самый продаваемый жанр в Европе в {year} был {top_genre_eu}')
        print(f'Самые популярные по численности игр платформы в Северной Америке в {year} были {top_platform_na}')
        print(f'Самые высокие средние продажи в Японии в {year} были у {top_avg_sales_publisher_jp}')
        print(f'Количество игр, которые более популярны в Европе чем в Японии в {year} составляет {popular_in_eu}')
     
    
    year_data = get_data()
    
    top_sales = most_sales_game(year_data)
    top_genre_eu = most_sales_genre_eu(year_data)
    top_platform_na = most_count_games_platform_na(year_data)
    top_avg_sales_publisher_jp = most_avg_sales_publisher_jp(year_data)
    popular_in_eu = game_count_more_popular_in_eu(year_data)
    
    print_data(top_sales, top_genre_eu, top_platform_na, top_avg_sales_publisher_jp, popular_in_eu)
    
semradionov2 = semradionov2()