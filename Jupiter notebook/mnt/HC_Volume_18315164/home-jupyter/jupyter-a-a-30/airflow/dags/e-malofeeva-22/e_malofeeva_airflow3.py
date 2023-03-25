import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task

default_args = {
    'owner': 'e.malofeeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 31),
    'schedule_interval' : '0 8 * * *'
}

year = 1994 + hash(f'e-malofeeva-22') % 23
link = 'https://drive.google.com/file/d/1dqesdMfFgoAX9gHfOmZ-sU5Rp66Hxbdr/view?usp=drivesdk'
path = 'https://drive.google.com/uc?export=download&id='+link.split('/')[-2]

@dag(default_args = default_args, catchup = False)
def e_malofeeva_airflow3():
    # таска считывания файла
    @task()
    def get_data():
        df = pd.read_csv(path)
        df = df.query('Year == @year')
        return df

    # Какая игра была самой продаваемой во всем мире?
    @task()
    def most_popular_name(df):
        most_popular_name = df.groupby('Name', as_index = False) \
        .agg({'Global_Sales' : 'sum'}) \
        .query('Global_Sales == Global_Sales.max()')['Name'] \
        .reset_index(drop = True)
        return most_popular_name

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def most_popular_genre_eu(df):
        most_popular_genre_eu = df.groupby('Genre', as_index = False) \
        .agg({'EU_Sales' : 'sum'}) \
        .query('EU_Sales == EU_Sales.max()')['Genre'] \
        .reset_index(drop = True)
        return most_popular_genre_eu

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def most_popular_platform_na(df):
        most_popular_platform_na = df.groupby('Platform', as_index = False) \
        .agg({'NA_Sales' : 'sum'}) \
        .sort_values('NA_Sales', ascending = False) \
        .query('NA_Sales > 1')
        return most_popular_platform_na

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def most_popular_publisher_jp(df):
        most_popular_publisher_jp = df.groupby('Publisher', as_index = False) \
        .agg({'JP_Sales' : 'mean'}) \
        .query('JP_Sales == JP_Sales.max()')['Publisher'] \
        .reset_index(drop = True)

        return most_popular_publisher_jp

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def df_sales_jp_eu(df):
        df_sales_jp = df.groupby('Name', as_index = False) \
        .agg({'JP_Sales' : 'sum'})
        df_sales_eu = df.groupby('Name', as_index = False) \
        .agg({'EU_Sales' : 'sum'})
        df_sales_jp_eu = df_sales_jp.merge(df_sales_eu)
        df_sales_jp_eu['diff_sales'] = df_sales_jp_eu.EU_Sales - df_sales_jp_eu.JP_Sales
        games_eu = df_sales_jp_eu.query('diff_sales > 0').shape[0]
        return games_eu

    @task()
    def print_data(most_popular_name, most_popular_genre_eu, most_popular_platform_na, most_popular_publisher_jp, games_eu):
        

        print(f'Most popular game in {year}')
        print(most_popular_name)

        print(f'Most popular genre in Europe in {year}')
        print(most_popular_genre_eu)

        print(f'Most popular  plarform in USA in {year}')
        print(most_popular_platform_na)

        print(f'Most popular publisher in Japan in {year}')
        print(most_popular_publisher_jp)

        print(f'Games with sales in Europe better than in Japan in {year}')
        print(games_eu)
        
    df = get_data()
    most_popular_name = most_popular_name(df)
    most_popular_genre_eu = most_popular_genre_eu(df)
    most_popular_platform_na = most_popular_platform_na(df)
    most_popular_publisher_jp = most_popular_publisher_jp(df)
    games_eu = df_sales_jp_eu(df)
    print_data(most_popular_name, most_popular_genre_eu, most_popular_platform_na, most_popular_publisher_jp, games_eu)   
    
e_malofeeva_airflow3 = e_malofeeva_airflow3()