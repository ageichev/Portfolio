#!/usr/bin/env python
# coding: utf-8

# In[28]:


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


# In[29]:


file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'{"m-ahmadiev"}') % 23


# In[30]:


default_args = {
    'owner': 'm-ahmadiev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 4),
    'schedule_interval': '0 15 * * *'
}


# In[31]:


@dag(default_args=default_args)
def Task_3_m_ahmadiev():
    @task()
    def get_data(file):
        df = pd.read_csv(file)
        df = df[df['Year']== year].reset_index(drop=True)
        return df

    # Самая продаваемая в этом году игра во всем мире
    @task()
    def get_best_selling_game(df):
        top_game = df.groupby("Name",as_index=False).agg({'Global_Sales':'sum'})         .sort_values('Global_Sales',ascending=False).round(2)

        best_selling_game = top_game[top_game['Global_Sales']==top_game.Global_Sales.max()]         .Name.to_csv(index=False, header=False)

        return best_selling_game

    # Жанры игр были самые продаваемые в Европе
    @task()
    def get_best_selling_genre(df):
        top_genre = df.groupby("Genre",as_index=False).agg({'EU_Sales':'sum'}).sort_values('EU_Sales',ascending=False).round(2)

        best_selling_genre = top_genre[top_genre['EU_Sales']==top_genre.EU_Sales.max()]         .Genre.to_csv(index=False, header=False)

        return best_selling_genre

    # Платформа, на которой продались игры более чем миллионным тиражом в Северной Америке
    @task()
    def get_top_platform_in_NA(df):
        top_platform = df[df['NA_Sales'] > 1].groupby("Platform",as_index=False)         .agg({'NA_Sales':'count'}).sort_values('NA_Sales',ascending=False)

        platform = top_platform[top_platform['NA_Sales']==top_platform.NA_Sales.max()]         .Platform.to_csv(index=False, header=False)

        return platform

    # Издатель с самыми высокими средними продажами в Японии
    @task()
    def get_top_publisher_in_jp(df):
        top_publisher = df.groupby("Publisher",as_index=False).agg({'JP_Sales':'mean'})         .sort_values('JP_Sales',ascending=False).round(2)

        publisher = top_publisher[top_publisher['JP_Sales']==top_publisher.JP_Sales.max()]         .Publisher.to_csv(index=False, header=False)

        return publisher

    # Количество игр, которые продались лучше в Европе, чем в Японии
    @task()
    def get_number_of_games(df):
        top_game = df.groupby('Name',as_index=False).agg({'EU_Sales':sum, 'JP_Sales':sum})
        number_of_games = top_game[top_game['EU_Sales'] > top_game['JP_Sales']].shape[0]

        return number_of_games
    @task()
    def print_data(best_selling_game, best_selling_genre, platform, publisher, number_of_games):
        print(f'The best-selling game in {year} in the world: {best_selling_game}')
        print(f'The best-selling genre in {year} in Europe: {best_selling_genre}')
        print(f'Top platform games with a million copie in {year} in NA: {platform}')
        print(f'Publishers with the highest average sales in {year} in Japan: {publisher}')
        print(f'The number of games sold is better in Europe than in Japan in {year}: {number_of_games}')
       
    data = get_data(file)
    best_selling_game = get_best_selling_game(data)
    best_selling_genre = get_best_selling_genre(data)
    platform = get_top_platform_in_NA(data)
    publisher = get_top_publisher_in_jp(data)
    number_of_games = get_number_of_games(data)   
    print_data(best_selling_game, best_selling_genre, platform, publisher, number_of_games)

Task_3_m_ahmadiev = Task_3_m_ahmadiev()


# In[ ]:




