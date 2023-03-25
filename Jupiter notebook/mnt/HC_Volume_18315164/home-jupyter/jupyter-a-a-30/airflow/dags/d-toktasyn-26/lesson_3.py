import pandas as pd
import numpy as np
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import timedelta, datetime
from io import StringIO


default_args ={
    'owner' : 'd-toktasyn-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 19),
    'schedule_interval': '0 12 * * *'
}
login='d-toktasyn-26'
my_year = 1994 + hash(f'{login}') % 23

@dag(default_args=default_args)
def a_top_games_by_d_toktasyn_26():
    @task()
    def get_df():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        return df
    
    @task()
    def get_global_high(df):
        return df.sort_values('Global_Sales', ascending=False).Name.values[0]
    
    @task()
    def get_top_genres_eu(df):
        eu_top = df.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False).head(1)
        eu_top_genres = eu_top['Genre'].tolist()
        genre = " ".join(eu_top_genres)
        return genre
    
    @task()
    def get_top_platform_na(df):
        na_top = df.groupby('Platform', as_index=False).agg({'NA_Sales':'count'}).sort_values('NA_Sales', ascending=False).head(1)
        top_platform = na_top['Platform'].tolist()
        na_top_platform = ", ".join(top_platform)
        return na_top_platform
    
    @task()
    def get_top_pub_jp(df):
        jp = df.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending=False).head(1)
        jp_top = jp['Publisher'].tolist()
        jp_top = ", ".join(jp_top)
        return jp_top
    
    @task()
    def diff_eu_jp(df):
        eu_jp = df.groupby('Name', as_index=False).agg({'EU_Sales':'sum', 'JP_Sales':'sum'}).sort_values('EU_Sales')
        return int(eu_jp.query('(EU_Sales - JP_Sales)>0').shape[0])
    
    @task()
    def print_df(global_high, eu_top_genres, na_top_platform, jp_top, diff):
        print(f'Statistics about games in {my_year}:')
        print(f'Top game in the world: {global_high}')
        print(f'Top genre in Europe: {eu_top_genres}')
        print(f'Top platform in NA: {na_top_platform}')
        print(f'Top publisher in Japan: {jp_top}')
        print(f'How many games sold better in Europe than in Japan: {diff}')

        
    df = get_df()
    global_high = get_global_high(df)
    eu_top_genres = get_top_genres_eu(df)
    na_top_platform = get_top_platform_na(df)
    jp_top = get_top_pub_jp(df)
    diff = diff_eu_jp(df)
    
    print_df(global_high, eu_top_genres, na_top_platform, jp_top, diff)
    
a_top_games_by_d_toktasyn_26 = a_top_games_by_d_toktasyn_26()