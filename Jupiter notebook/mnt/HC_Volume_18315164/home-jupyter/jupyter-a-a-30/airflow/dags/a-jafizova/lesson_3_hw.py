
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



df_sales_path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


default_args = {
    'owner': 'a.jafizova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 12),
}

schedule_interval = '0 12 * * *'

login = 'a-jafizova'
year = 1994 + hash(f'{login}') % 23


@dag(default_args=default_args, catchup=False, schedule_interval = schedule_interval)
def games_airflow_Aig():
    @task()
    def get_data(retries = 3):
        df_sales = pd.read_csv(df_sales_path)
        df_sales = df_sales[df_sales.Year == year]
        return df_sales


    @task()
    def max_sales(df_sales):
        game_max_sale = df_sales.groupby('Name', as_index = False).agg({'Global_Sales':'sum'}).max()
        return game_max_sale
    
    @task()
    def genre_sales(df_sales):
        genre_eu_sales = df_sales.groupby('Genre', as_index = False).agg({'EU_Sales':'sum'}).max()
        return genre_eu_sales
    
    @task()
    def games_platform (df_sales):
        games_na_platform = df_sales.query('NA_Sales > 1.00').groupby('Platform', as_index = False).agg({'Name':'count'}).max()
        return games_na_platform
    
    @task()
    def sales_ja_max (df_sales):
        sales_max_japan = df_sales.groupby('Publisher', as_index = False).agg({'JP_Sales':'sum'}).max()
        return sales_max_japan
    
    @task()
    def sales_eu_japan (df_sales):
        games_eu_japan = df_sales.query('EU_Sales > JP_Sales').groupby('Name', as_index = False).agg({'Name':'count'}).sum()
        return games_eu_japan
    

    @task()
    def print_data(game_max_sale, genre_eu_sales, games_na_platform, sales_max_japan, games_eu_japan):

        context = get_current_context()
        date = context['ds']

        print(f'The most selling game for {date}')
        print(game_max_sale)
        
        print(f'The most selling genre in EU for {date}')
        print(genre_eu_sales)
        
        print(f'The most selling platform in NA for {date}')
        print(games_na_platform)
        
        print(f'The most selling publisher in Japan for {date}')
        print(sales_max_japan)
        
        print(f'The number of better selling games in EU than in Japan for {date}')
        print(games_eu_japan)
        


    df_sales = get_data()
    game_max_sale = max_sales(df_sales)
    genre_eu_sales = genre_sales(df_sales)
    games_na_platform = games_platform (df_sales)
    sales_max_japan = sales_ja_max (df_sales)
    games_eu_japan = sales_eu_japan (df_sales)
    print_data(game_max_sale, genre_eu_sales, games_na_platform, sales_max_japan, games_eu_japan)

games_airflow_Aig = games_airflow_Aig()

