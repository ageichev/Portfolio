import requests
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


year = float(1994 + hash(f'{"s-frolova-30"}') % 23)

default_args = {
    'owner': 's-frolova-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 20),
    'schedule_interval': '0 10 * * *'
}


@dag(default_args=default_args, catchup=False)
def s_frolova_30_lesson_3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df.query('Year == @year')
        return df

    @task(retries=4, retry_delay=timedelta(10))
    def best_sold_game(df):
        df_1 = df.groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}).sort_values('Global_Sales', ascending=False)
        best_sold_game_data = df_1.Name.head(1)
        return best_sold_game_data

    @task()
    def popular_genre(df):
        df_2 = df.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .query('EU_Sales == EU_Sales.max()')
        popular_genre_data = df_2.Genre
        return popular_genre_data

    @task()
    def platform_great_solds_in_NA(df):
        df_3 = df.query('NA_Sales > 1.00').groupby('Platform', as_index=False) \
            .agg({'Name': 'nunique'}) \
            .query('Name == Name.max()')
        platform_great_solds_in_NA_data = df_3.Platform
        return platform_great_solds_in_NA_data

    @task()
    def publisher_avg_sales_Japan(df):
        df_4 = df.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .query('JP_Sales == JP_Sales.max()')
        publisher_avg_sales_Japan_data = df_4.Publisher
        return publisher_avg_sales_Japan_data
    
    @task()
    def EU_sales_better_than_JP(df):
        df_5 = df.groupby('Name', as_index=False) \
            .agg({'JP_Sales': 'sum', 'EU_Sales': 'sum'}) \
            .query('EU_Sales > JP_Sales') 
        EU_sales_better_than_JP_data = df_5.shape[0]
        return EU_sales_better_than_JP_data

    @task()
    def print_data(best_sold_game_data, popular_genre_data, platform_great_solds_in_NA_data, publisher_avg_sales_Japan_data, EU_sales_better_than_JP_data):

        print(f'''Самая продаваемая игра в {year} году: {best_sold_game_data}''')
        print(f'''Игры жанра(ов) {popular_genre_data} были самыми продаваемыми в Европе в {year} году''')   
        print(f'''В {year} году в Северной Америке больше всего игр, которые продались более чем миллионным тиражом, были на платформе {platform_great_solds_in_NA_data}''') 
        print(f'''В {year} году самые высокие средние продажи в Японии у издателя {publisher_avg_sales_Japan_data}''') 
        print(f'''В {year} в Европе продались лучше, чем в Японии {EU_sales_better_than_JP_data} игр''') 
        

    df = get_data()
   
    best_sold_game_data = best_sold_game(df)
    popular_genre_data = popular_genre(df)
    platform_great_solds_in_NA_data = platform_great_solds_in_NA(df)
    publisher_avg_sales_Japan_data = publisher_avg_sales_Japan(df)
    EU_sales_better_than_JP_data = EU_sales_better_than_JP(df)
    
    print_data(best_sold_game_data, popular_genre_data, platform_great_solds_in_NA_data, publisher_avg_sales_Japan_data, EU_sales_better_than_JP_data)


    
s_frolova_30_lesson_3 = s_frolova_30_lesson_3()
