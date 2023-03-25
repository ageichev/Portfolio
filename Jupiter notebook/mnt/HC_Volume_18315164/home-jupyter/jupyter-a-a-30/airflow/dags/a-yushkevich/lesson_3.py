import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


year = 1994 + hash('a-yushkevich') % 23
path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {'owner': 'a.yushkevich',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=2),
                'start_date': datetime(2023, 3, 1)}


@dag(default_args=default_args, schedule_interval='0 13 * * *', catchup=False)
def a_yushkevich_lesson_3():
    @task(retries=2)
    def get_data():
        df = pd.read_csv(path)
        df_2008 = df.query('Year == @year').reset_index(drop=True)
        return df_2008

    @task(retries=2)
    def get_popular_game(df_2008):
        pop_game = df_2008.groupby('Name', as_index=False) \
                              .agg({'Global_Sales': 'sum'}) \
                              .rename(columns={'Global_Sales': 'Sum_sales'}) \
                              .sort_values('Sum_sales', ascending=False) \
                              .query('Sum_sales == Sum_sales.max()') \
                              .reset_index(drop=True)
        return pop_game

    @task(retries=2)
    def get_europe_genres(df_2008):
        eur_genres = df_2008.groupby('Genre', as_index=False) \
                            .agg({'EU_Sales': 'sum'}) \
                            .rename(columns={'EU_Sales': 'Sum_sales'}) \
                            .sort_values('Sum_sales', ascending=False) \
                            .query('Sum_sales == Sum_sales.max()') \
                            .reset_index(drop=True)
        return eur_genres

    @task(retries=2)
    def get_na_platform(df_2008):
        na_platform_sales = df_2008.query('NA_Sales > 1') \
                                   .groupby('Platform', as_index=False) \
                                   .agg({'Name': 'count'}) \
                                   .rename(columns={'Name': 'Sales'}) \
                                   .sort_values('Sales', ascending=False) \
                                   .query('Sales == Sales.max()') \
                                   .reset_index(drop=True)
        return na_platform_sales

    @task(retries=2)
    def get_jp_avg_sales(df_2008):
        jp_avg_publ_sales = df_2008.query('JP_Sales > 0') \
                                   .groupby('Publisher', as_index=False) \
                                   .agg({'JP_Sales': 'mean'}) \
                                   .rename(columns={'JP_Sales': 'Mean_sales'}) \
                                   .sort_values('Mean_sales', ascending=False) \
                                   .query('Mean_sales == Mean_sales.max()') \
                                   .round(2) \
                                   .reset_index(drop=True) 
        return jp_avg_publ_sales
 
    @task(retries=2)
    def get_eu_better_jp(df_2008):
        df_2008_eu_vs_jp = df_2008
        df_2008_eu_vs_jp['Sales_difference'] = df_2008_eu_vs_jp['EU_Sales'] - df_2008_eu_vs_jp['JP_Sales']
        eur_better_jp = df_2008_eu_vs_jp.query('Sales_difference > 0') \
                                        .groupby('Name', as_index=False) \
                                        .agg({'Publisher': 'count'}) \
                                        .Name.count()
        return eur_better_jp

    @task(retries=2)
    def print_data(a, b, c, d, e):
        print(f'The best-selling game in the world in {year} was the game "{a.Name[0]}".')
        print(f'\nIn {year}, the best-selling games were of the genre "{b.Genre[0]}".')
        print(f'\nIn North America in {year} in platform "{c.Platform[0]}" had the most games that sold over a million copies.')
        print(f'\nIn Japan, in {year}, the highest average sales were from the publisher "{d.Publisher[0]}".')
        print(f'\nIn {year} {e} games sold better in Europe than in Japan')
        

    df_2008 = get_data()
    pop_game = get_popular_game(df_2008)
    eur_genres = get_europe_genres(df_2008)
    na_platform_sales  = get_na_platform(df_2008)
    jp_avg_publ_sales = get_jp_avg_sales(df_2008)
    eur_better_jp = get_eu_better_jp(df_2008)
    print_data(pop_game, eur_genres, na_platform_sales, jp_avg_publ_sales, eur_better_jp)
    
a_yushkevich_lesson_3 = a_yushkevich_lesson_3()
