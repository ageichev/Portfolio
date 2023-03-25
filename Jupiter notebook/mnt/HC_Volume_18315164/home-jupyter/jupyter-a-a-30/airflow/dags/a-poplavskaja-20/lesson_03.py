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


default_args = {
    'owner': 'a-poplavskaja-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 7, 4),
    'schedule_interval': '5 * * * *'
}

year = 1994 + hash(f'{"a-poplavskaja-20"}') % 23

@dag(default_args=default_args)
def lesson_3_a_poplavskaja_20():
    
# Получаем данные
    @task()
    def get_data():
        file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        df_game = pd.read_csv(file).query('Year == @year')
        return df_game

# Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_game_sale(df_game):
        best_game = df_game[df_game.Global_Sales == df_game.Global_Sales.max()].Name.values[0]
        return best_game

# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_best_genre_EU(df_game):
        best_genre = df_game[df_game.EU_Sales == df_game.EU_Sales.max()].Genre.to_list()
        return best_genre
    
# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def get_best_platform_NA(df_game):
        Platform_NA = df_game.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'NA_Sales': 'count'})
        best_Platform = Platform_NA[Platform_NA.NA_Sales == Platform_NA.NA_Sales.max()].Platform.to_list()
        return best_Platform

# У какого издателя самые высокие средние продажи в Японии?
    @task()
    def get_best_publisher_JP(df_game):
        Publisher = df_game.groupby('Publisher',as_index=False).agg({'JP_Sales':'mean'})
        best_Publisher = Publisher[Publisher['JP_Sales']==Publisher.JP_Sales.max()].Publisher.to_list()
        return best_Publisher

# Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_EU_better_sale_JP(df_game):
        EU_JP = df_game.groupby('Name',as_index=False).agg({'EU_Sales':sum, 'JP_Sales':sum})
        country_better_sale = EU_JP.query('EU_Sales > JP_Sales').shape[0]
        return country_better_sale

# Выводим на печать результаты
    @task()
    def print_data(get_best_game_sale, get_best_genre_EU, get_best_platform_NA, get_best_publisher_JP, get_EU_better_sale_JP):
        print(f'The best game sale in {year}: {get_best_game_sale}')
        print(f'The best genre EU in {year}: {get_best_genre_EU}')
        print(f'Top best platform NA in {year}: {get_best_platform_NA}')
        print(f'The best publisher JP in {year}: {get_best_publisher_JP}')
        print(f'The game better sale EU than JP in {year}: {get_EU_better_sale_JP}')

    data = get_data()
    best_game_sale = get_best_game_sale(data)
    best_genre_EU = get_best_genre_EU(data)
    best_platform_NA = get_best_platform_NA(data)
    best_publisher_JP = get_best_publisher_JP(data)
    EU_better_sale_JP = get_EU_better_sale_JP(data)   
    print_data(get_best_game_sale, get_best_genre_EU, get_best_platform_NA, get_best_publisher_JP, get_EU_better_sale_JP)
    
lesson_3_a_poplavskaja_20 = lesson_3_a_poplavskaja_20()
