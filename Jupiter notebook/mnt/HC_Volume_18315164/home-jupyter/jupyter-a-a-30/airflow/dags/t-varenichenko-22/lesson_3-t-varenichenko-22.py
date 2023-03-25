#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import csv

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

link = 'https://drive.google.com/file/d/1dqesdMfFgoAX9gHfOmZ-sU5Rp66Hxbdr/view?usp=drivesdk'
path = 'https://drive.google.com/uc?export=download&id='+link.split('/')[-2]

my_year = 1994 + hash(f't-varenichenko-22') % 23

default_args = {
    'owner': 't.varenichenko-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 8, 3),
}
schedule_interval = '0 */4 * 8 *'


@dag(default_args=default_args, catchup=False)
def lesson_3_games_t_varenichenko_22():
    @task(retries=2)
    def get_data():
        games_data = pd.read_csv(path)
        games_data = games_data.query('Year == @my_year')
        return games_data
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=2)
    def get_best_sales(games_data):
        best_sales = games_data.groupby('Name', as_index = False)                                 .agg({'Global_Sales': 'sum'})                                 .rename(columns={'Global_Sales': 'Sales'})                                 .sort_values(by='Sales', ascending=False)                                 .Name.iloc[0]
        return best_sales
    
    # Игры какого жанра были самыми продаваемыми в Европе?    
    @task(retries=2)
    def get_top_eu_genres(games_data):
        genres = games_data.groupby('Genre', as_index=False)                           .agg({'EU_Sales': 'sum'})                           .sort_values('EU_Sales', ascending=False)    
        top_eu_sales = genres.EU_Sales.iloc[0]
        top_eu_genres = genres.query('EU_Sales == @top_eu_sales').Genre.tolist()
        return top_eu_genres
 
    # На какой платформе было больше всего игр, которые продались более чем 
    # миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task(retries=2)
    def get_top_platform(games_data):
        platform = games_data.query("NA_Sales > 1")                   .groupby('Platform', as_index=False)                   .agg({'NA_Sales': 'count'})                   .sort_values('NA_Sales', ascending=False)
        top_na_sales = platform['NA_Sales'].iloc[0]
        top_platform = platform.query('NA_Sales == @top_na_sales').Platform.tolist()
        return top_platform
    
    # У какого издателя самые высокие средние продажи в Японии? 
    @task()
    def get_top_publisher_jp(games_data):
        publisher = games_data.groupby('Publisher', as_index=False)                              .agg({'JP_Sales': 'mean'})                              .sort_values('JP_Sales', ascending=False)
        top_jp_sales = publisher.JP_Sales.iloc[0]
        top_publisher_jp = publisher[publisher['JP_Sales'] == top_jp_sales].Publisher.tolist()
        return top_publisher_jp
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=2)
    def get_eu_vs_jp(games_data):
        eu_vs_jp = games_data.groupby('Name')                              .agg({'JP_Sales': 'sum', 'EU_Sales': 'sum'})                              .query("EU_Sales > JP_Sales").shape[0]
        return eu_vs_jp
    
    @task(retries=2)  
    def print_data(best_sales, 
                   top_eu_genres, 
                   top_platform,
                   top_publisher_jp,
                   eu_vs_jp):
        context = get_current_context()
        date = context['ds']
        print(f"В {my_year} году самой продаваемой игрой была {best_sales}")
        print(f"В {my_year} году в Европе наибольшие продажи принесли игры жанра(ов) {', '.join(top_eu_genres)}")
        print(f"В {my_year} году в Северной Америке наибольшие продажи принесли игры на платформе(ах) {', '.join(top_platform)}")
        print(f"В {my_year} году в Японии наибольшие продажи игр от издателя(ей) {', '.join(top_publisher_jp)}")
        print(f"В {my_year} году в Европе на {eu_vs_jp} копий игр продали больше, чем в Японии")

    games_data = get_data()
    best_sales = get_best_sales(games_data)
    top_eu_genres = get_top_eu_genres(games_data)
    top_platform = get_top_platform(games_data)
    top_publisher_jp = get_top_publisher_jp(games_data)
    eu_vs_jp = get_eu_vs_jp(games_data)
    print_data(best_sales, top_eu_genres, top_platform, top_publisher_jp, eu_vs_jp)

lesson_3_games_t_varenichenko_22 = lesson_3_games_t_varenichenko_22()

