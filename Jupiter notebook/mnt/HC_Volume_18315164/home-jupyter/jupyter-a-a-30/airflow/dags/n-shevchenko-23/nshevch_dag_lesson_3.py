#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context


# In[2]:


login = 'n-shevchenko-23'
my_year  =1994 + hash(f'{login}') % 23


# In[3]:


default_args = {
    'owner': 'n-shevchenko-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 21),
    'schedule_interval' : '0 13 * * *'
}


# In[7]:


@dag(default_args=default_args, catchup=False)
def nshev_dag_lesson_3():
    @task(retries = 3)
    def nshev_get_data():   
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv') 
        df.columns = df.columns.str.lower()
        df_my = df[df.year == my_year]
        return df_my.to_csv(index = False)

    #popular_game_global
    @task(retries = 2)
    def nshev_pop_game(df_my):
        df_my = pd.read_csv(StringIO(df_my))
        popular_game_global = df_my.groupby('name')['global_sales'].sum().idxmax()
        return  popular_game_global

    #popular_genre_europe
    @task(retries = 2)
    def nshev_eu_pop_genre(df_my):
        df_my = pd.read_csv(StringIO(df_my))
        popular_genre_eu = df_my.groupby('genre')['eu_sales'].sum().idxmax()
        return popular_genre_eu

    #platform_us_over_1_million
    @task(retries = 2)
    def nshev_us_platform(df_my):
        df_my = pd.read_csv(StringIO(df_my))
        platform_na_gr = df_my[df_my.na_sales > 1].groupby('platform').name.count().idxmax()
        return platform_na_gr

    #publisher_with_biggest_avr_sales_in_japan
    @task(retries = 2)
    def nshev_publish_jp(df_my):
        df_my = pd.read_csv(StringIO(df_my))
        publisher_with_biggest_avr_sales = df_my.groupby('publisher')['jp_sales'].mean().idxmax()
        return publisher_with_biggest_avr_sales

    #game_sold_better_in_eu_vs_jp_count
    @task(retries = 2)
    def nshev_eu_vs_jp(df_my):
        df_my = pd.read_csv(StringIO(df_my))
        eu_jp = df_my.groupby('name',as_index = False)['eu_sales','jp_sales'].sum()
        eu_jp['eu_more'] =eu_jp['eu_sales'] > eu_jp['jp_sales']
        count_games_sold_better_in_eu = eu_jp['eu_more'].sum()
        return count_games_sold_better_in_eu


    @task(retries = 2)
    def print_data(popular_game_global,
                   popular_genre_eu,
                   platform_na_gr,
                   publisher_with_biggest_avr_sales,
                   count_games_sold_better_in_eu
                  ):
        context = get_current_context()
        date = context['ds']

        print(f'''Дата для {date}:
        Самая продаваемая игра в {my_year} году во всем мире: {popular_game_global}.
        Самая продаваемый жанр игр в {my_year} году в Европе: {popular_genre_eu}.
        На платформе {platform_na_gr} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {my_year}.
        Издатель с самыми высокими средними продажами в {my_year} году в Японии: {publisher_with_biggest_avr_sales}.
        {count_games_sold_better_in_eu} игры продалось лучше в Европе, чем в Японии в {my_year} году.
        ''')
    #dag info
    df = nshev_get_data()
    popular_game_global = nshev_pop_game(df)
    popular_genre_eu = nshev_eu_pop_genre(df)
    platform_na_gr = nshev_us_platform(df)
    publisher_with_biggest_avr_sales = nshev_publish_jp(df)
    count_games_sold_better_in_eu = nshev_eu_vs_jp(df)

    print_data(popular_game_global, 
               popular_genre_eu, 
               platform_na_gr, 
               publisher_with_biggest_avr_sales, 
               count_games_sold_better_in_eu)

nshev_dag_lesson_3 = nshev_dag_lesson_3()

    
    
    

