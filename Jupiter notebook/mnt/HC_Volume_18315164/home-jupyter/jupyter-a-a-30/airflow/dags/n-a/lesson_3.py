import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 'n-a',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 10),
    'schedule_interval': '0 10 * * *'
}

@dag(default_args=default_args, catchup=False)
def nanokhina_dag_3():
    @task(retries=3)
    def get_data():
        login = 'n-a'
        year=1994 + hash(f'{login}') % 23
        df_games = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv').query('Year==@year')
        return df_games
    
    @task()
    def get_game(df_games):
        max_sales_game = df_games.groupby('Name', as_index=False).agg({'Global_Sales': 'sum'}).sort_values('Global_Sales', ascending=False)['Name'].iloc[0]
        return max_sales_game
    
    @task()
    def get_genre(df_games):
        max_sales_genre = df_games.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending=False)['Genre'].iloc[0]
        return max_sales_genre
    
    @task()
    def get_platform(df_games):
        max_sales_platform = df_games.query("NA_Sales>1").groupby('Platform', as_index=False).agg({'Name': 'count'}).sort_values('Name', ascending=False)['Platform'].iloc[0]
        return max_sales_platform
    
    @task()
    def get_publisher(df_games):
        max_sales_publisher = df_games.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False)['Publisher'].iloc[0]
        return max_sales_publisher
    
    @task()
    def get_eu_jp_games(df_games):
        eu_jp_games = df_games.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).query("EU_Sales > JP_Sales")['Name'].nunique()
        return eu_jp_games
    
    @task()
    def print_data(max_sales_game, max_sales_genre, max_sales_platform, max_sales_publisher, eu_jp_games):
        print (f'Data for year: {year}')
        print (f'The best selling game: {max_sales_game}')
        print (f'The best selling genre in EU: {max_sales_genre}')
        print (f'The best selling platform in NA: {max_sales_platform}')
        print (f'The publisher with the highest average sales in JP: {max_sales_publisher}')
        print (f'The amount of games better selling in EU then in JP: {eu_jp_games}')   
        
    df_games = get_data()
    max_sales_game = get_game(df_games)
    max_sales_genre = get_genre(df_games)
    max_sales_platform = get_platform(df_games)
    max_sales_publisher = get_publisher(df_games)
    eu_jp_games = get_eu_jp_games(df_games)
    print_data(max_sales_game, max_sales_genre, max_sales_platform, max_sales_publisher, eu_jp_games)

nanokhina_dag_3 = nanokhina_dag_3()
