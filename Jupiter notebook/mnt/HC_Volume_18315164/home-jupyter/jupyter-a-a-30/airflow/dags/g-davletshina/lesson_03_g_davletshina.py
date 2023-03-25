#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'g-davletshina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 30),
}
schedule_interval = '0 12 * * *'

login = 'g-davletshina'
year = 1994 + hash(f"{login}") % 23

@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def g_davletshina_lesson3():
    
    @task(retries=3)
    def get_data():
        data = pd.read_csv(path)
        data = data.query("Year == @year")
        return data

    @task(retries=4)
    def world_game(data):
        world_top_game = data.groupby("Name").agg({"Global_Sales": "sum"}).idxmax()[0]
        return world_top_game
    
    @task()
    def eu_top_genres (data):
        eu_game_genres = data.groupby("Genre").agg({"EU_Sales": "sum"}).idxmax()[0]
        return eu_game_genres

    @task()
    def na_platform_top(data):
        na_platform = data[data['NA_Sales'] > 1].groupby(["Platform", "Name"]).agg({"NA_Sales": "sum"}).idxmax()[0][0]
        return na_platform

    @task()
    def top_publisher_japan(data):
        jp_publisher = data.groupby("Publisher").agg({"JP_Sales": "mean"}).idxmax()[0]
        return jp_publisher

    @task()
    def eu_jp_sales_game(data):
        eu_jp_sales = data.groupby("Name").agg({"EU_Sales": "sum", "JP_Sales": "sum"}).query("EU_Sales > JP_Sales").shape[0]
        return eu_jp_sales

    @task()
    def print_data(world_game, eu_top_genres, na_platform_top, top_publisher_japan, eu_jp_sales_game):
        context = get_current_context()
        date = context['ds']
        print(f'For{date}.Это самая продаваемая игра во всем мире в {year} году')
        print(world_game)
        print(f'For{date}.Игры этого жанра были самыми продаваемыми в Европе в {year} году')
        print(eu_top_genres)
        print(f'For{date}.На этой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {year} году')
        print(na_platform_top)
        print(f'For{date}.У этого издателя самые высокие средние продажи в Японии в {year} году')
        print(top_publisher_japan)
        print(f'For{date}.Количество игр, которые продались лучше в Европе, чем в Японии в {year} году')
        print(eu_jp_sales_game)


    data = get_data()
    world_game_ = world_game(data)
    eu_top_genres_ = eu_top_genres(data)
    na_platform_top_ = na_platform_top(data)
    top_publisher_japan_ = top_publisher_japan(data)
    eu_jp_sales_game_ = eu_jp_sales_game(data)
    print_data(world_game_, eu_top_genres_, na_platform_top_, top_publisher_japan_, eu_jp_sales_game_)

g_davletshina_lesson3 = g_davletshina_lesson3()


# In[ ]:




