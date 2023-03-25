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

#data = pd.read_csv('vgsales.csv')
#data = data[data['Year'] == 1994]

default_args = {
    'owner': 'a-jakovlev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 5),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args = default_args, catchup = False)
def a_jakovlev_2():
    
    @task()
    def get_data():
        data = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        year = 1994 + hash(f'a-jakovlev') % 23
        data = data[data['Year'] == year]
        return data
        
    #Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def best_seller_game(data):
        return data.loc[data['Global_Sales'].idxmax(),"Name"]


    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def best_seller_genre_eu(data):
        eu_sales_by_genre = data.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'})
        return eu_sales_by_genre.loc[eu_sales_by_genre['EU_Sales'].idxmax(), "Genre"]


    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def best_seller_platforn_na(data):
        en_games_by_platform = data.query('NA_Sales >= 1') \
            .groupby('Platform', as_index=False)["Name"].count().rename(columns={'Name':"count"})
        return en_games_by_platform.loc[en_games_by_platform['count'].idxmax() ,"Platform"]


    # У какого издателя самые высокие средние продажи в Японии?
    @task()
    def best_seller_publisher_jp(data):
        jp_sales_by_publisher = data.groupby('Publisher', as_index=False).JP_Sales.mean()
        return jp_sales_by_publisher.loc[jp_sales_by_publisher['JP_Sales'].idxmax(),"Publisher"]


    #Сколько игр продались лучше в Европе, чем в Японии
    @task()
    def better_in_eu_that_in_jp(data):
        return data.query("EU_Sales > JP_Sales").Name.count()
    
    @task()
    def print_results(res1, res2, res3, res4, res5):
        context = get_current_context()
        date = context['ds']
        print(f'Данные на {date}')
        print(f'самой продаваемой в этом году во всем мире: {res1}')
        print(f'Жанр игр, которые были самыми продаваемыми в Европе: {res2}')
        print(f'Платформа с мамым большим числом игр, которые продались более чем миллионным тиражом в Северной Америке: {res3}')
        print(f'Издатель с самыми высокими средними продажами в Японии: {res4}')
        print(f'Количество игр, которые продались лучше в Европе, чем в Японии: {res5}')
        
    
    data = get_data()
    res1 = best_seller_game(data)
    res2 = best_seller_genre_eu(data)
    res3 = best_seller_platforn_na(data)
    res4 = best_seller_publisher_jp(data)
    res5 = better_in_eu_that_in_jp(data)
    print_results(res1, res2, res3, res4, res5)

a_jakovlev_2 = a_jakovlev_2()