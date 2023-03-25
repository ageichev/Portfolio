from tokenize import String
import requests
import numpy as np
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import  StringIO
from airflow.operators.python import get_current_context 

from airflow.decorators import dag, task

my_year = 1994 + hash(f't-hajnatskaja-21') % 23


default_args = {
    'owner': 't-hajnatskaja-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 12 * * *'
    }

@dag(default_args=default_args, catchup=False)
def khaynatskaya_hw_3():
    @task()
    def get_data():
        df = pd.read_csv('vgsales.csv').query('Year == @my_year')
        df = df.rename(columns=lambda x: x.lower())
        return df

    #  Какая игра была самой продаваемой в этом году во всем мире?

    @task()
    def get_bestseller(df):
        bestseller = df[df.global_sales == df.global_sales.max()].name.values[0]
        return bestseller

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько

    @task()
    def get_bestseller_genre_eu(df):
        bestseller_genre_eu = df.groupby('genre', as_index = False).agg({'eu_sales':'sum'}).sort_values('eu_sales', ascending=False).head(3)
        bestseller_genre_eu = bestseller_genre_eu.genre.to_list()
        return bestseller_genre_eu

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def get_plaform_na(df):
        na_platform = df.query('na_sales > 1').groupby('platform', as_index = False).agg({'na_sales':'sum', 'name':'count'}).sort_values('name', ascending = False).head(3)
        na_platform = na_platform.platform.to_list()
        return na_platform

    # У какого издателя самые высокие средние продажи в Японии? 
    @task()
    def get_publisher_jp(df):
        publisher_jp = df.groupby('publisher', as_index = False).agg({'jp_sales':'mean'}).sort_values('jp_sales', ascending = False).head(3)
        publisher_jp = publisher_jp.publisher.to_list()
        return publisher_jp

    # Сколько игр продались лучше в Европе, чем в Японии? 
    @task()
    def get_number_saled_eu_jp(df):
        eu_jp = df.query('eu_sales > jp_sales').shape[0] 
        return eu_jp 

    @task()
    def print_data(bestseller_game, top_genres, top_platform, top_publisher, difference): 

        context = get_current_context()
        date = context['ds']

        bestseller = bestseller_game['bestseller']
        bestseller_genre_eu = top_genres['bestseller_genre_eu']
        na_platform = top_platform['top_platform']
        publisher_jp = top_publisher['publisher_jp']
        eu_jp = difference['eu_jp']

        print(f'''Data of {my_year} for {date}
                Bestseller Game all over the world: {bestseller}
                Top 3 EU Genres: {bestseller_genre_eu}
                Top 3 NA Platform: {na_platform}
                Top 3 JP Publishers: {publisher_jp}
                Difference between EU and JP : {eu_jp}''')


    df = get_data()

    bestseller = get_bestseller(df)
    bestseller_genre_eu = get_bestseller_genre_eu(df)
    na_platform = get_plaform_na(df)
    publisher_jp = get_publisher_jp(df)
    difference_eu_jp = get_number_saled_eu_jp(df)

    print_data(bestseller, bestseller_genre_eu, na_platform, publisher_jp, difference_eu_jp)

khaynatskaya_hw_3 = khaynatskaya_hw_3()