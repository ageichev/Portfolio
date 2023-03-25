#!/usr/bin/env python
# coding: utf-8

# In[43]:


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

default_args = {
    'owner': 'f-imamagzam',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 2),
    'schedule_interval': '0 6 * * *'
}

@dag(default_args=default_args, catchup=False)
def task_3_imamagzam():
    @task()
    def get_data():
        df=pd.read_csv('vgsales.csv')
        return df

    @task()

    def get_year():
        year=1994 + hash(f'b-imamagzam') % 23
        return year

    # Какая игра была самой продаваемой в этом году во всем мире?

    @task()
    def get_most_popular_game():
        most_popular_game = df.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).sort_values('Global_Sales',ascending=False).head(1)['Name']
        return most_popular_game


    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько

    @task()
    def get_most_popular_game_EU():
        most_popular_game_EU = df.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).nlargest(50,'EU_Sales',keep='all').Genre.to_list()
        return most_popular_game_EU

    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?

    @task()
    def get_platform():
        platform = df.query('NA_Sales>1').groupby('Platform', as_index=False).agg({'Name':'nunique'}).nlargest(1,'Name',keep='all').Platform.to_list()
        return platform

    #У какого издателя самые высокие средние продажи в Японии?

    @task
    def get_sales_japan():
        sales_japan=df.groupby('Publisher',as_index=False).agg({'JP_Sales':'mean'}).nlargest(1,'JP_Sales',keep='all').Publisher.to_list()
        return sales_japan


    #Сколько игр продались лучше в Европе, чем в Японии?

    @task
    def get_best_sale_europe():
        best_sale_europe=df.groupby('Name',as_index=False).agg({'EU_Sales':'sum','JP_Sales':'sum'}).query('EU_Sales > JP_Sales').Name.count()
        return best_sale_europe

    @task
    def print_data(df,year, most_popular_game, most_popular_game_EU, platform, sales_japan, best_sale_europe):
        print('Data', df.head(4))
        print(f'Data for the year {year}')
        print(f'The most popular game {most_popular_game}')
        print(f'The most popular game in EU {most_popular_game_EU}')
        print(f'Platform with the highest 1 mln selling game in NA  {platform}')
        print(f'The publisher with the highest mean sales in JP  {sales_japan}')
        print(f'Count of the games between JP and EU{best_sale_europe}')


    df=get_data()
    year=get_year()
    most_popular_game=get_most_popular_game()
    most_popular_game_EU=get_most_popular_game_EU()
    platform=get_platform()
    sales_japan=get_sales_japan()
    best_sale_europe=get_best_sale_europe()
    print_data()

task_3_imamagzam=task_3_imamagzam()