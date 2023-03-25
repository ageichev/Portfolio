#!/usr/bin/env python
# coding: utf-8

# In[38]:


import requests
from zipfile import ZipFile
import io
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 2),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def game_report():
    
    @task(retries=3)
    def get_data():
        df = pd.read_csv(vgsales)
        return df

    @task(retries=4, retry_delay=timedelta(10))
    def get_game_name(df):
        game_name = df.query('Year == 2002')                        .groupby('Name')                        .agg({'Global_Sales':'sum'})                        .reset_index()                        .sort_values(by='Global_Sales', ascending=False)                        .iloc[0,0]
        return game_name

    @task()
    def get_genre(df):
        genre = df.query('Year == 2002')                            .groupby('Genre')                            .agg({'EU_Sales':'sum'})                            .reset_index()                            .sort_values(by='EU_Sales',ascending=False)                            .iloc[0:3,0:2]
        return {genre.iloc[0,0]: genre.iloc[0,1]}

    @task()
    def na_sales(df):
        na = df.query('Year == 2002' and 'NA_Sales > 1')              .groupby('Platform').agg({'Name':'count'})              .reset_index()              .sort_values(by='Name',ascending=False)              .iloc[0:3,0:2]

        na_s = {na.iloc[0,0]: na.iloc[0,1],na.iloc[1,0]:na.iloc[1,1], na.iloc[2,0]:na.iloc[2,1]}

        return na_s

    @task()
    def publisher_sales(df):
        s = df.query('Year == 2002')              .groupby('Publisher')              .agg({'JP_Sales':'mean'})              .reset_index()              .sort_values(by='JP_Sales', ascending=False)              .iloc[0:2,0:2]

        pub_s = {s.iloc[0,0]:s.iloc[0,1], s.iloc[1,0]:s.iloc[1,1]}

        return pub_s

    @task()
    def comparing_ja_na(df):
        prep = df.query('Year == 2002').groupby('Name').agg({'EU_Sales':'sum', 'JP_Sales':'sum'}).reset_index()
        filtered = prep[prep['EU_Sales']>prep['JP_Sales']].shape[0]
        return filtered

    @task()
    def print_data(game_name, genre, na_s, pub_s, filtered):

        print(f'Самая продаваемая игра в мире — {game_name}')
        print(f'Самые продаваемые жанры игр в Европе — {genre}')
        print(f'Платформы, на которых было продано больше всего игр миллионным тиражом в Северной Америке — {na_s}')
        print(f'Издатель, у которого самые высокие средние продажи в Японии — {pub_s}')
        print(f'{filtered} игр продалось больше в Европе, чем в Японии')
        
    df = get_data()

    game_name = get_game_name(df)
    genre = get_genre(df)
    na_s = na_sales(df)
    pub_s = publisher_sales(df)
    filtered = comparing_ja_na(df)

    print_data(game_name, genre, na_s, pub_s, filtered)

game_report = game_report()


# In[ ]:




