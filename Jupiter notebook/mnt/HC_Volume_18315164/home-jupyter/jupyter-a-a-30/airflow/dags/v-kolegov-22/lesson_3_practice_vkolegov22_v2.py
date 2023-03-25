#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
# import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'v-kolegov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 7, 26)
}

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'v-kolegov-22'
year = 1994 +  hash('{login}') % 23 

@dag(default_args=default_args, catchup=False)
def games_vkolegov22_hw():
    @task(retries=4)
    def get_data():
        top_games = pd.read_csv('vgsales.csv')
        return top_games

    @task(retries=4)
    def get_popular_in_world(df):
        df_popular_in_world = df.query('Year == @year').groupby('Name').agg('Global_Sales').sum().reset_index().sort_values('Global_Sales', ascending=False)
        popular_in_world = df_popular_in_world.loc[df_popular_in_world['Global_Sales'].idxmax().tolist()]
        return popular_in_world
    
    
    @task(retries=4)
    def get_genre_in_eu(df):
        df_genre_in_eu = df.query('Year == @year').groupby('Genre').agg('EU_Sales').sum().reset_index().sort_values('EU_Sales', ascending=False)
        genre_in_eu = df_genre_in_eu.loc[df_genre_in_eu['EU_Sales'].idxmax().tolist()]
        return genre_in_eu
    
    @task(retries=4)
    def get_platform_na(df):
        df_platform_na = df.query('Year == @year').query('NA_Sales > 1').groupby('Platform').agg('NA_Sales').sum().reset_index().sort_values('NA_Sales', ascending=False)
        platform_na = df_platform_na.loc[df_platform_na['NA_Sales'].idxmax().tolist()]
        return platform_na

    @task(retries=4)
    def get_publisher_jp(df):
        df_publisher_jp = df.query('Year == @year').groupby('Publisher').agg('JP_Sales').mean().reset_index().sort_values('JP_Sales', ascending=False)
        publisher_jp = df_publisher_jp.loc[df_publisher_jp['JP_Sales'].idxmax().tolist()]
        return publisher_jp
     
    @task(retries=4)
    def get_eu_than_jp(df):
        eu_than_jp = df.query('Year == @year').groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).reset_index().query('EU_Sales > JP_Sales').shape[0]
        return eu_than_jp
    

    @task(retries=4)
    def print_data(popular_in_world, genre_in_eu, platform_na, publisher_jp, eu_than_jp):    

        context = get_current_context()
        date = context['ds']

        print(f'The most popular in world in {year}')
        print(popular_in_world)

        print(f'The most popular genre in EU in {year}')
        print(genre_in_eu)
        
        print(f'The most popular platform in NA in {year}')
        print(platform_na)
        
        print(f'The most popular publisher in Japan in {year}')
        print(publisher_jp)
        
        print(f'How many games sold better in EU rather than in Japan in {year}')
        print(eu_than_jp)
        
        
    df = get_data()
    
    popular_in_world = get_popular_in_world(df)
    genre_in_eu = get_genre_in_eu(df)
    platform_na = get_platform_na(df)
    publisher_jp = get_publisher_jp(df)
    eu_than_jp = get_eu_than_jp(df)

    print_data(popular_in_world, genre_in_eu, platform_na, publisher_jp, eu_than_jp)

games_vkolegov22_hw = games_vkolegov22_hw()



