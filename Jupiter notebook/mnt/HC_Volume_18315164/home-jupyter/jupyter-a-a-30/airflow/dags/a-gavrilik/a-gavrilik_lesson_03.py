# # На этом мы заканчиваем изучение Airflow
# 
# **В качестве последнего задания попробуем использовать Airflow для решения аналитических задач.**
# 
# **Будем использовать следующие [данные](https://git.lab.karpov.courses/lab/airflow/-/blob/master/dags/a.batalov/vgsales.csv)**
# 
# ## Задания:
# 
# **Сначала определим год, за какой будем смотреть данные.**
# 
# **Сделать это можно так: в питоне выполнить `1994 + hash(f‘{login}') % 23`,  где {login} - ваш логин (или же папка с дагами)**
# 
# **Дальше нужно составить DAG из нескольких тасок, в результате которого нужно будет найти ответы на следующие вопросы:**
# 
# 1. Какая игра была самой продаваемой в этом году во всем мире?
# 2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
# 3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
# 4. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
# 5. Сколько игр продались лучше в Европе, чем в Японии?
# 
# **Оформлять DAG можно как угодно, важно чтобы финальный таск писал в лог ответ на каждый вопрос.**
# 
# **Ожидается, что в DAG будет 7 тасков. По одному на каждый вопрос, таск с загрузкой данных и финальный таск который собирает все ответы.**
# 
# **Дополнительный бонус за настройку отправки сообщений в телеграмм по окончанию работы DAG**


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

year = 1994 + hash(f'a-gavrilik') % 23

default_args = {
    'owner': 'a-gavrilik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 28),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def a_gavrilik_lesson3():
    # ## 0. Получение данных
    @task()
    def get_data():
        games = pd.read_csv('vgsales.csv')
        return games


    # ## 1. Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def best_selling_game(games):
        best_game = games \
            .query('Year == @year') \
            [['Name', 'Global_Sales']] \
            .sort_values('Global_Sales', ascending=False) \
            .head(1) \
            .reset_index(drop=True)
        
        return best_game


    # ## 2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def best_selling_genre(games):
        max_eu_sales = games \
            .query('Year == @year') \
            [['Genre', 'EU_Sales']] \
            .groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .EU_Sales \
            .max() 

        best_genre = games \
            .query('Year == @year') \
            [['Genre', 'EU_Sales']] \
            .groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .query('EU_Sales == @max_eu_sales') \
            .reset_index(drop=True)

        return best_genre


    # ## 3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def best_platform_north_america_sales(games):
        max_na_sales = games \
            .query('Year == @year') \
            [['Platform', 'NA_Sales']] \
            .query('NA_Sales > 1') \
            .groupby('Platform', as_index=False) \
            .agg({'NA_Sales': 'sum'}) \
            .NA_Sales \
            .max()

        best_platform_na_sales = games \
            .query('Year == @year') \
            [['Platform', 'NA_Sales']] \
            .query('NA_Sales > 1') \
            .groupby('Platform', as_index=False) \
            .agg({'NA_Sales': 'sum'}) \
            .query('NA_Sales == @max_na_sales') \
            .reset_index(drop=True)

        return best_platform_na_sales


    # ## 4. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def best_publisher_by_avg_japan_sales(games):
        max_avg_jp_sales = games \
            .query('Year == @year') \
            [['Publisher', 'JP_Sales']] \
            .groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .JP_Sales \
            .max()

        best_publisher_jp_sales = games \
            .query('Year == @year') \
            [['Publisher', 'JP_Sales']] \
            .groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .query('JP_Sales == @max_avg_jp_sales') \
            .reset_index(drop=True)

        return best_publisher_jp_sales


    # ## 5. Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def eu_jp_sales_diff(games):
        df = games

        df['diff'] = df['EU_Sales'] - df['JP_Sales']

        count_games = df \
            .query('Year == @year') \
            [['Name', 'diff']] \
            .query('diff > 0') \
            .shape[0]

        return count_games


    # ## 6. Печать данных
    @task()
    def print_data(best_game, best_genre, best_platform_na_sales, best_publisher_jp_sales, count_games):

        print(f'Best Selling Game in {year}')
        print(best_game)
        print()

        print(f'Best Selling Genre in Europe in {year}')
        print(best_genre)
        print()

        print(f'Best Selling Platform in Nort America in {year}')
        print(best_platform_na_sales)
        print()

        print(f'Best Publisher by Avg Sales in Japan in {year}')
        print(best_publisher_jp_sales)
        print()

        print(f'Count of Games whith Europe Sales Better then Japan in {year}')
        print(count_games)
        
        
    games = get_data()
    
    best_game = best_selling_game(games)
    best_genre = best_selling_genre(games)
    best_platform_na_sales = best_platform_north_america_sales(games)
    best_publisher_jp_sales = best_publisher_by_avg_japan_sales(games)
    count_games = eu_jp_sales_diff(games)
    
    print_data(best_game, best_genre, best_platform_na_sales, best_publisher_jp_sales, count_games)

a_gavrilik_lesson3 = a_gavrilik_lesson3()