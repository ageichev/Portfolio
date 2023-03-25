#!/usr/bin/env python
# coding: utf-8


# In[ ]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

path  = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

year = 1994 + hash(f'{"i-dashkevich-25"}')%23


default_args = {
    'owner': 'i-dashkevich-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 8),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def airflow_lesson_3():
    
    @task()
    def get_games_data():
        df = pd.read_csv(path)
        games_data = df[df.Year == year]
        return games_data

    @task()
    def top_sales_game(games_data):
        top_game = games_data.groupby('Name', as_index = False)             .agg({'Global_Sales' : 'sum'}).sort_values('Global_Sales',ascending = False).head(1)
        return top_game
        
    @task()
    def top_genre_in_EU(games_data):
        top_genre_in_EU = games_data.groupby('Genre', as_index = False)             .agg({'EU_Sales' : 'sum'}).sort_values('EU_Sales', ascending =False).head(1)
        return top_genre_in_EU
        
    @task()
    def top_in_NA_platform(games_data):
        top_in_NA_platform = games_data.query('NA_Sales > 1')             .groupby('Platform', as_index = False).agg({'Name' : 'count'}).sort_values('Name', ascending = False) 
        return top_in_NA_platform

    @task()
    def top_in_JP_publisher(games_data):
        top_in_JP_publisher = games_data.groupby('Publisher', as_index = False)             .agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending=False).head(5)
        return top_in_JP_publisher
    
    @task()
    def EU_JP_sales(games_data):
        EU_JP_sales = games_data.query('EU_Sales > JP_Sales').shape[0]
        return EU_JP_sales


    @task()
    def print_data(top_game, top_genre_in_EU, top_in_NA_platform, top_in_JP_publisher, EU_JP_sales):
        
        context = get_current_context()
        date = context['ds']

        
        print(f'Top game by Global Sales for {date}')
        print(top_game)
        
        print(f'Top genre game in EU Sales for {date}')
        print(top_genre_in_EU)
        
        print(f'Top game platform in NA Sales for {date}')
        print(top_in_NA_platform)
        
        print(f'Top publisher in JP Sales for {date}')
        print(top_in_JP_publisher)
        
        print(f'Count sales games of EU  more than JP for {date}')
        print(EU_JP_sales)

    games_data = get_games_data()
    top_game = top_sales_game(games_data)
    top_genre_in_EU = top_genre_in_EU(games_data)
    top_in_NA_platform = top_in_NA_platform(games_data)
    top_in_JP_publisher = top_in_JP_publisher(games_data)
    EU_JP_sales = EU_JP_sales(games_data)
    
    print_data(top_game, top_genre_in_EU, top_in_NA_platform, top_in_JP_publisher, EU_JP_sales)
       
airflow_lesson_3 = airflow_lesson_3()

