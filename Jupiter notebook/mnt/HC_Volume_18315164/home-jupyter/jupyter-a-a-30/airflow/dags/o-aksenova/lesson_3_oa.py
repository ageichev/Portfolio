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

path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'o-aksenova'
my_year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'o.aksenova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '0 15 * * *'
}

@dag(default_args = default_args, catchup = False)
def lesson_3_airflow_oaksenova():
    
    @task(retries = 3)
    def read_data_oa():
        df = pd.read_csv(path_to_file)
        df = df.dropna(subset = ['Year'])
        return df
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_seller_game(df):
        best_seller_game = df.query('Year == @my_year') \
                                .sort_values('Global_Sales', ascending = False) \
                                .head(1)
        t1 = best_seller_game['Name'].iloc[0]
        return t1
    
    # Игры какого жанра были самыми продаваемыми в Европе? 
    @task()
    def get_best_seller_genre_eu(df):
        best_seller_eu = df.query('Year == @my_year') \
                    .groupby('Genre') \
                    .agg({'EU_Sales':'sum'}) \
                    .sort_values('EU_Sales', ascending = False) \
                    .reset_index().head(1)
        t2 = best_seller_eu['Genre'].iloc[0]
        return t2
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def get_na_platform_mio_sales(df):
        northam_platform = df.query('Year == @my_year') \
                        .groupby('Platform') \
                        .agg({'NA_Sales':'sum'}) \
                        .sort_values('NA_Sales', ascending = False) \
                        .reset_index()
        t3 = northam_platform.query('NA_Sales >= 1')['Platform'].to_list()
        return t3

    # У какого издателя самые высокие средние продажи в Японии?
    @task()
    def get_avg_publish_sales_jp(df):
        t4 = df.query('Year == @my_year') \
                            .groupby('Publisher') \
                            .agg({'JP_Sales':'mean'}) \
                            .sort_values('JP_Sales', ascending = False) \
                            .reset_index().head(1)
        return t4
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_better_sales_eu_vs_jp(df):
        t5 = len(df.query('(EU_Sales > JP_Sales) & (Year == @my_year)'))
        return t5
    
    @task()
    def print_data_oa(t1, t2, t3, t4, t5):
        
        login = 'o-aksenova'
        year = 1994 + hash(f'{login}') % 23
        

        print(f'1. Bestseller game for {year} is {t1}' )

        print(f'2. Bestseller genre in EU for {year} is {t2}')
        
        print(f'3. Top platforms in NA in mio sales for {year}: {t3}')
        
        print(f'4. Top publisher in Avg Sales in JP for {year}: {t4}')
        
        print(f'5. Number of games with more sales in EU than in JP for {year}: {t5}')

    df = read_data_oa()
    t1 = get_best_seller_game(df)
    t2 = get_best_seller_genre_eu(df)
    t3 = get_na_platform_mio_sales(df)
    t4 = get_avg_publish_sales_jp(df)
    t5 = get_better_sales_eu_vs_jp(df)
    print_data_oa(t1, t2, t3, t4, t5)

lesson_3_airflow_oaksenova = lesson_3_airflow_oaksenova()